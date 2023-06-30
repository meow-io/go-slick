package slick

import (
	"bytes"
	"database/sql"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/data/eav"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/messaging"
	"github.com/meow-io/go-slick/migration"
	"go.uber.org/zap"
)

type DeviceGroupInvite struct {
	Invite   *messaging.Jpake1 `bencode:"i"`
	Password string            `bencode:"s"`
}

type baseStruct struct {
	ID           []byte  `db:"id"`
	GroupID      []byte  `db:"group_id"`
	CtimeSec     float64 `db:"_ctime"`
	MtimeSec     float64 `db:"_mtime"`
	WtimeSec     float64 `db:"_wtime"`
	IdentityID   []byte  `db:"_identity_tag"`
	MembershipID []byte  `db:"_membership_tag"`
}

type Device struct {
	baseStruct

	Name string `db:"name"`
	Type string `db:"type"`
}

type membership struct {
	baseStruct

	OriginGroupID      []byte `db:"origin_group_id"`
	OriginIdentityID   []byte `db:"origin_identity_id"`
	OriginMembershipID []byte `db:"origin_membership_id"`
	Membership         []byte `db:"membership"`
}

type proposal struct {
	baseStruct

	ApplierGroupID       []byte `db:"applier_group_id"`
	ApplierIdentityID    []byte `db:"applier_identity_id"`
	ApplierMembershipID  []byte `db:"applier_membership_id"`
	ProposedMembershipID []byte `db:"proposed_membership_id"`
	ProposedMembership   []byte `db:"proposed_membership"`
}

type deviceGroup struct {
	log               *zap.SugaredLogger
	slick             *Slick
	deviceGroup       *Group
	membershipUpdater messaging.MembershipUpdater
	ID                ids.ID
}

func newDeviceGroup(slick *Slick) (*deviceGroup, error) {
	log := slick.config.Logger("device_group")
	deviceGroupID := slick.messaging.DeviceGroupID()
	if err := slick.DB.Migrate("_device_group", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				if err := slick.EAVCreateView("_dg_memberships", &eav.ViewDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"origin_group_id": {
							SourceName: "memberships_origin_group_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"origin_identity_id": {
							SourceName: "memberships_origin_identity_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"origin_membership_id": {
							SourceName: "memberships_origin_membership_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"membership": {
							SourceName: "memberships_membership",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
					},
					Indexes: [][]string{},
				}); err != nil {
					return err
				}

				if err := slick.EAVCreateView("_dg_proposals", &eav.ViewDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"applier_identity_id": {
							SourceName: "proposals_applier_identity_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"applier_membership_id": {
							SourceName: "proposals_applier_membership_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"applier_group_id": {
							SourceName: "proposals_applier_group_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"proposed_membership_id": {
							SourceName: "proposals_proposed_membership_id",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
						"proposed_membership": {
							SourceName: "proposals_proposed_membership",
							ColumnType: eav.Blob,
							Required:   true,
							Nullable:   false,
						},
					},
					Indexes: [][]string{},
				}); err != nil {
					return err
				}

				return slick.EAVCreateView("_dg_devices", &eav.ViewDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"name": {
							SourceName: "devices_name",
							ColumnType: eav.Text,
							Required:   true,
							Nullable:   false,
						},
						"type": {
							SourceName: "devices_type",
							ColumnType: eav.Text,
							Required:   true,
							Nullable:   false,
						},
					},
					Indexes: [][]string{},
				})
			},
		},
	}); err != nil {
		return nil, err
	}

	g, err := slick.Group(deviceGroupID)
	if err != nil {
		return nil, err
	}

	updater := func(groupID, identityID, membershipID ids.ID, membershipDesc *messaging.Membership) error {
		if groupID == deviceGroupID {
			return nil
		}

		memberships := make([]*membership, 0)
		if err := slick.selectEAV(
			&memberships,
			"select * FROM _dg_memberships where group_id = ? AND origin_group_id = ? AND _identity_tag = ? AND _membership_tag = ?",
			g.ID[:],
			groupID[:],
			g.IdentityTag[:],
			g.MembershipTag[:],
		); err != nil {
			return err
		}

		var id ids.ID
		if len(memberships) == 0 {
			var err error
			id, err = slick.NewID(g.AuthorTag)
			if err != nil {
				return err
			}
		} else {
			id = ids.IDFromBytes(memberships[0].ID)
		}

		membershipDescBytes, err := bencode.Serialize(&membershipDesc)
		if err != nil {
			return err
		}

		if len(memberships) != 0 && bytes.Equal(membershipDescBytes, memberships[0].Membership) {
			return nil
		}
		writer := slick.EAVWriter(g)
		writer.Update("_dg_memberships", id[:], map[string]interface{}{
			"origin_group_id":      groupID[:],
			"origin_identity_id":   identityID[:],
			"origin_membership_id": membershipID[:],
			"membership":           membershipDescBytes,
		})
		return writer.execute()
	}

	if err := slick.EAVSubscribeBeforeEntity(func(_ string, groupID, entityID ids.ID) error {
		if groupID != deviceGroupID {
			return nil
		}

		p := &proposal{}
		if err := slick.getEAV(p, "select * from _dg_proposals WHERE id = ? AND group_id = ?", entityID[:], groupID[:]); err != nil {
			return err
		}

		if bytes.Equal(p.IdentityID, g.IdentityTag[:]) && bytes.Equal(p.MembershipID[:], g.MembershipTag[:]) {
			return nil
		}

		mem := &messaging.Membership{}
		if err := bencode.Deserialize(p.ProposedMembership, mem); err != nil {
			log.Warnf("error deserializing membership proposal %#v", err)
			return nil
		}

		return slick.messaging.ApplyProposal(
			ids.IDFromBytes(p.ApplierGroupID),
			ids.IDFromBytes(p.ApplierIdentityID),
			ids.IDFromBytes(p.ApplierMembershipID),
			ids.IDFromBytes(p.ProposedMembershipID),
			mem,
		)
	}, true, "_dg_proposals"); err != nil {
		return nil, err
	}

	if err := slick.EAVSubscribeBeforeEntity(func(_ string, groupID, entityID ids.ID) error {
		if groupID != deviceGroupID {
			return nil
		}
		m := &membership{}
		if err := slick.getEAV(m, "select * from _dg_memberships WHERE id = ? AND group_id = ?", entityID[:], groupID[:]); err != nil {
			return err
		}
		mem := &messaging.Membership{}
		if err := bencode.Deserialize(m.Membership, mem); err != nil {
			log.Warnf("error deserializing membership %#v", err)
			return nil
		}
		if err := slick.addMembership(ids.IDFromBytes(m.OriginGroupID), ids.IDFromBytes(m.OriginIdentityID), ids.IDFromBytes(m.OriginMembershipID), mem); err != nil {
			return err
		}
		existingMemberships := make([]*membership, 0)
		if err := slick.selectEAV(&existingMemberships, "select * from _dg_memberships WHERE group_id = ? AND origin_identity_id = ? AND _identity_tag = ? AND _membership_tag = ?", g.ID[:], m.OriginIdentityID, g.IdentityTag[:], g.MembershipTag[:]); err != nil {
			return err
		}
		if len(existingMemberships) != 0 {
			return nil
		}
		if bytes.Equal(m.IdentityID, g.IdentityTag[:]) && bytes.Equal(m.MembershipID, g.MembershipTag[:]) {
			return nil
		}
		proposals := make([]*proposal, 0)
		if err := slick.selectEAV(
			&proposals,
			"select * FROM _dg_proposals where group_id = ? AND applier_group_id = ? AND applier_identity_id = ? AND _identity_tag = ? AND _membership_tag = ?",
			deviceGroupID[:],
			m.OriginGroupID[:],
			m.OriginIdentityID[:],
			g.IdentityTag[:],
			g.MembershipTag[:],
		); err != nil {
			return err
		}
		if len(proposals) != 0 {
			return nil
		}
		id, err := slick.NewID(g.AuthorTag)
		if err != nil {
			return err
		}
		proposedMembershipID, proposedMembership, err := slick.proposeMembership(ids.IDFromBytes(m.OriginGroupID), ids.IDFromBytes(m.OriginIdentityID), ids.IDFromBytes(m.OriginMembershipID), mem)
		if err != nil {
			return err
		}
		proposedMembershipBytes, err := bencode.Serialize(proposedMembership)
		if err != nil {
			return err
		}
		writer := slick.EAVWriter(g)
		writer.Update("_dg_proposals", id[:], map[string]interface{}{
			"applier_identity_id":    m.OriginIdentityID,
			"applier_membership_id":  m.OriginMembershipID,
			"applier_group_id":       m.OriginGroupID,
			"proposed_membership_id": proposedMembershipID[:],
			"proposed_membership":    proposedMembershipBytes,
		})
		return writer.execute()
	}, true, "_dg_memberships"); err != nil {
		return nil, err
	}

	return &deviceGroup{log, slick, g, updater, g.ID}, nil
}

func (dg *deviceGroup) SetNameType(name, ty string) error {
	devices := make([]*Device, 0)
	if err := dg.slick.EAVSelect(&devices, "select * FROM _dg_devices where group_id = ? AND _identity_tag = ? AND _membership_tag = ? order by name, type", dg.deviceGroup.ID[:], dg.deviceGroup.IdentityTag[:], dg.deviceGroup.MembershipTag[:]); err != nil {
		return err
	}

	var id ids.ID
	if len(devices) == 0 {
		var err error
		id, err = dg.slick.NewID(dg.deviceGroup.AuthorTag)
		if err != nil {
			return err
		}
	} else {
		id = ids.IDFromBytes(devices[0].ID)
	}

	writer := dg.slick.EAVWriter(dg.deviceGroup)
	writer.Update("_dg_devices", id[:], map[string]interface{}{
		"name": name,
		"type": ty,
	})
	return writer.Execute()
}

func (dg *deviceGroup) NameType() (deviceName, deviceType string, err error) {
	result, err := dg.slick.EAVQuery("select name, type from _dg_devices where group_id = ? AND _identity_tag = ? AND _membership_tag = ?", dg.deviceGroup.ID[:], dg.deviceGroup.IdentityTag[:], dg.deviceGroup.MembershipTag[:])
	if err != nil {
		return
	}

	if len(result.Rows) == 0 {
		return
	}

	deviceName = result.Rows[0][0].(string)
	deviceType = result.Rows[0][1].(string)
	return
}

func (dg *deviceGroup) GetDeviceLink() (*DeviceGroupInvite, error) {
	var invite *messaging.Jpake1
	password, err := NewPin()
	if err != err {
		return nil, err
	}
	if err := dg.slick.DB.Run("get device link", func() error {
		var err error
		invite, err = dg.slick.messaging.InviteMember(dg.deviceGroup.ID, password)
		return err
	}); err != nil {
		return nil, err
	}

	return &DeviceGroupInvite{invite, password}, nil
}

func (dg *deviceGroup) LinkDevice(dgi *DeviceGroupInvite) error {
	return dg.slick.DB.Run("link device", func() error {
		id := dg.deviceGroup.ID
		_, err := dg.slick.messaging.ApproveJpakeInviteWithID(dgi.Invite, dgi.Password, &id)
		return err
	})
}

func (dg *deviceGroup) Devices() ([]*Device, error) {
	var devices []*Device
	return devices, dg.slick.EAVSelect(&devices, "select * from _dg_devices where group_id = ? order by name", dg.deviceGroup.ID[:])
}

func (dg *deviceGroup) Count() (count int, err error) {
	if err = dg.slick.DB.Run("count members", func() error {
		members, err := dg.slick.messaging.GroupMembers(dg.deviceGroup.ID)
		if err != nil {
			return err
		}
		count = len(members)
		return nil
	}); err != nil {
		return
	}
	return
}
