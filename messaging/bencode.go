package messaging

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/ids"
)

const (
	envelopeRatchetMessage = 0
	envelopePrekey1        = 1
	envelopePrekey2        = 2
	envelopePrekey3        = 3
	envelopePrekey4        = 4
	envelopePrekey5        = 5
	envelopeJpake2         = 6
	envelopeJpake3         = 7
	envelopeJpake4         = 8
	envelopeJpake5         = 9
	envelopeJpake6         = 10

	lostBodyPrivate = 0
	lostBodyGroup   = 1

	privateMessageBackfillRequest  = 0
	privateMessageBackfillStart    = 1
	privateMessageBackfillBody     = 2
	privateMessageBackfillComplete = 3
	privateMessageBackfillAborted  = 4
	privateMessageRepairMessage    = 5

	backfillRequestTypeFull    = 0
	backfillRequestTypePartial = 1
)

type envelope struct {
	Type uint8  `bencode:"t"`
	Body []byte `bencode:"b"`
}

type EncryptedBody struct {
	PublicKey [32]byte `bencode:"k"`
	Body      []byte   `bencode:"b"`
}

func newEnvelopeWithPrekey1(pk1 *prekey1) (*envelope, error) {
	pk1Bytes, err := bencode.Serialize(pk1)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopePrekey1,
		Body: pk1Bytes,
	}, nil
}

func newEnvelopeWithPrekey2(pk2 *prekey2) (*envelope, error) {
	pk2Bytes, err := bencode.Serialize(pk2)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopePrekey2,
		Body: pk2Bytes,
	}, nil
}

func newEnvelopeWithPrekey3(pk3 *prekey3) (*envelope, error) {
	pk3Bytes, err := bencode.Serialize(pk3)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopePrekey3,
		Body: pk3Bytes,
	}, nil
}

func newEnvelopeWithPrekey4(pk4 *prekey4) (*envelope, error) {
	pk4Bytes, err := bencode.Serialize(pk4)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopePrekey4,
		Body: pk4Bytes,
	}, nil
}

func newEnvelopeWithPrekey5(pk5 *prekey5) (*envelope, error) {
	pk5Bytes, err := bencode.Serialize(pk5)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopePrekey5,
		Body: pk5Bytes,
	}, nil
}

func newEnvelopeWithJpake2(jp2 *jpake2) (*envelope, error) {
	jp2Bytes, err := bencode.Serialize(jp2)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopeJpake2,
		Body: jp2Bytes,
	}, nil
}

func newEnvelopeWithJpake3(jp3 *jpake3) (*envelope, error) {
	jp3Bytes, err := bencode.Serialize(jp3)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopeJpake3,
		Body: jp3Bytes,
	}, nil
}

func newEnvelopeWithJpake4(jp4 *jpake4) (*envelope, error) {
	jp4Bytes, err := bencode.Serialize(jp4)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopeJpake4,
		Body: jp4Bytes,
	}, nil
}

func newEnvelopeWithJpake5(jp5 *jpake5) (*envelope, error) {
	jp5Bytes, err := bencode.Serialize(jp5)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopeJpake5,
		Body: jp5Bytes,
	}, nil
}

func newEnvelopeWithJpake6(jp6 *jpake6) (*envelope, error) {
	jp6Bytes, err := bencode.Serialize(jp6)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopeJpake6,
		Body: jp6Bytes,
	}, nil
}

func newEnvelopeWithRatchetMessage(rm *ratchetMessage) (*envelope, error) {
	rmBytes, err := bencode.Serialize(rm)
	if err != nil {
		return nil, err
	}

	return &envelope{
		Type: envelopeRatchetMessage,
		Body: rmBytes,
	}, nil
}

func (e *envelope) decodePrekey1() (*prekey1, error) {
	if e.Type != envelopePrekey1 {
		return nil, fmt.Errorf("messaging: expected %d (envelopePrekey1), got %d", envelopePrekey1, e.Type)
	}

	pk1 := &prekey1{}
	return pk1, bencode.Deserialize(e.Body, pk1)
}

func (e *envelope) decodePrekey2() (*prekey2, error) {
	if e.Type != envelopePrekey2 {
		return nil, fmt.Errorf("messaging: expected %d (envelopePrekey2), got %d", envelopePrekey2, e.Type)
	}

	pk2 := &prekey2{}
	return pk2, bencode.Deserialize(e.Body, pk2)
}

func (e *envelope) decodePrekey3() (*prekey3, error) {
	if e.Type != envelopePrekey3 {
		return nil, fmt.Errorf("messaging: expected %d (envelopePrekey3), got %d", envelopePrekey3, e.Type)
	}

	pk3 := &prekey3{}
	return pk3, bencode.Deserialize(e.Body, pk3)
}

func (e *envelope) decodePrekey4() (*prekey4, error) {
	if e.Type != envelopePrekey4 {
		return nil, fmt.Errorf("messaging: expected %d (envelopePrekey4), got %d", envelopePrekey4, e.Type)
	}

	pk4 := &prekey4{}
	return pk4, bencode.Deserialize(e.Body, pk4)
}

func (e *envelope) decodePrekey5() (*prekey5, error) {
	if e.Type != envelopePrekey5 {
		return nil, fmt.Errorf("messaging: expected %d (envelopePrekey5), got %d", envelopePrekey5, e.Type)
	}

	pk5 := &prekey5{}
	return pk5, bencode.Deserialize(e.Body, pk5)
}

func (e *envelope) decodeJpake2() (*jpake2, error) {
	if e.Type != envelopeJpake2 {
		return nil, fmt.Errorf("messaging: expected %d (envelopeJpake2), got %d", envelopeJpake2, e.Type)
	}

	jp2 := &jpake2{}
	return jp2, bencode.Deserialize(e.Body, jp2)
}

func (e *envelope) decodeJpake3() (*jpake3, error) {
	if e.Type != envelopeJpake3 {
		return nil, fmt.Errorf("messaging: expected %d (envelopeJpake3), got %d", envelopeJpake3, e.Type)
	}

	jp3 := &jpake3{}
	return jp3, bencode.Deserialize(e.Body, jp3)
}

func (e *envelope) decodeJpake4() (*jpake4, error) {
	if e.Type != envelopeJpake4 {
		return nil, fmt.Errorf("messaging: expected %d (envelopeJpake4), got %d", envelopeJpake4, e.Type)
	}

	jp4 := &jpake4{}
	return jp4, bencode.Deserialize(e.Body, jp4)
}

func (e *envelope) decodeJpake5() (*jpake5, error) {
	if e.Type != envelopeJpake5 {
		return nil, fmt.Errorf("messaging: expected %d (envelopeJpake5), got %d", envelopeJpake5, e.Type)
	}

	jp5 := &jpake5{}
	return jp5, bencode.Deserialize(e.Body, jp5)
}

func (e *envelope) decodeJpake6() (*jpake6, error) {
	if e.Type != envelopeJpake6 {
		return nil, fmt.Errorf("messaging: expected %d (envelopeJpake6), got %d", envelopeJpake6, e.Type)
	}

	jp6 := &jpake6{}
	return jp6, bencode.Deserialize(e.Body, jp6)
}

func (e *envelope) decodeRatchetMessage() (*ratchetMessage, error) {
	if e.Type != envelopeRatchetMessage {
		return nil, fmt.Errorf("messaging: expected %d (ratchet message body), got %d", envelopeRatchetMessage, e.Type)
	}

	rm := &ratchetMessage{}
	return rm, bencode.Deserialize(e.Body, rm)
}

type ZKPBody struct {
	T []byte `bencode:"t"`
	R []byte `bencode:"r"`
}

type ratchetMessage struct {
	Dh   []byte `bencode:"dh"`
	N    uint32 `bencode:"n"`
	Pn   uint32 `bencode:"pn"`
	Body []byte `bencode:"b"`
}

type Jpake1 struct {
	ID        [16]byte                 `bencode:"id"`
	UserID    [16]byte                 `bencode:"u"`
	PublicKey [32]byte                 `bencode:"k"`
	X1G       []byte                   `bencode:"x1g"`
	X2G       []byte                   `bencode:"x2g"`
	X1ZKP     *ZKPBody                 `bencode:"x1zkp"`
	X2ZKP     *ZKPBody                 `bencode:"x2zkp"`
	ReplyTo   map[string]*EndpointInfo `bencode:"r"`
}

type jpake2 struct {
	ID        [16]byte                 `bencode:"id"`
	UserID    [16]byte                 `bencode:"u"`
	PublicKey [32]byte                 `bencode:"k"`
	X3G       []byte                   `bencode:"x3g"`
	X4G       []byte                   `bencode:"x4g"`
	B         []byte                   `bencode:"b"`
	XsZKP     *ZKPBody                 `bencode:"xszkp"`
	X3ZKP     *ZKPBody                 `bencode:"x3zkp"`
	X4ZKP     *ZKPBody                 `bencode:"x4zkp"`
	ReplyTo   map[string]*EndpointInfo `bencode:"r"`
}

type jpake3 struct {
	ID    [16]byte `bencode:"id"`
	A     []byte   `bencode:"a"`
	XsZKP *ZKPBody `bencode:"xszkp"`
}

type jpake4 struct {
	ID      [16]byte `bencode:"id"`
	Confirm []byte   `bencode:"c"`
}

type jpake5 struct {
	ID      [16]byte `bencode:"id"`
	Confirm []byte   `bencode:"c"`
	Inner   []byte   `bencode:"i"`
}

type jpake6 struct {
	ID    [16]byte `bencode:"id"`
	Inner []byte   `bencode:"i"`
}

type jpakeInner struct {
	Sig          [64]byte          `bencode:"s"`
	IdentityID   ids.ID            `bencode:"i"`
	MembershipID ids.ID            `bencode:"m"`
	Desc         *GroupDescription `bencode:"d"`
}

type prekey1 struct {
	Nonce     [16]byte `bencode:"n"`
	PublicKey [32]byte `bencode:"k"`
	Sig1      [64]byte `bencode:"s"`
}

type prekey2 struct {
	Nonce     [16]byte `bencode:"n"`
	PublicKey [32]byte `bencode:"k"`
	Sig2      [64]byte `bencode:"s"`
}

type prekey3 struct {
	Nonce [16]byte `bencode:"n"`
	Sig3  [64]byte `bencode:"s"`
}

type prekey4 struct {
	Nonce [16]byte `bencode:"n"`
	Desc1 []byte   `bencode:"d"`
}

type prekey5 struct {
	Nonce [16]byte `bencode:"n"`
	Desc2 []byte   `bencode:"d"`
}

type prekeyInner struct {
	Sig  [64]byte          `bencode:"s"`
	Desc *GroupDescription `bencode:"d"`
}

// // actual group messaging constructs

type GroupMessage struct {
	GroupMessages    []*GroupMessageBody `bencode:"b"`
	GroupAckSeq      uint64              `bencode:"gs"`
	GroupAckSparse   []byte              `bencode:"gss"`
	PrivateAckSeq    uint64              `bencode:"ps"`
	PrivateAckSparse []byte              `bencode:"pss"`
	BaseDigest       []byte              `bencode:"bd"`
	GroupChanges     []byte              `bencode:"gc"`
	NewDigest        []byte              `bencode:"nd"`
	PrivateMessages  []*PrivateMessage   `bencode:"m"`
	Lost             []*LostBody         `bencode:"l"`
}

type GroupMessageBody struct {
	Body                []byte              `bencode:"b"`
	Seq                 uint64              `bencode:"s"`
	UnhandledRecipients map[ids.ID][]ids.ID `bencode:"u"`
}

type LWWString struct {
	Value string `bencode:"v"`
	Time  uint64 `bencode:"t"`
}

func (d *LWWString) merge(o *LWWString) *LWWString {
	if o.Time > d.Time || (o.Time == d.Time && o.Value < d.Value) {
		return o
	}
	return d
}

type LWWBlob struct {
	Value []byte `bencode:"v"`
	Type  string `bencode:"y"`
	Time  uint64 `bencode:"t"`
}

func (d *LWWBlob) merge(o *LWWBlob) *LWWBlob {
	if o.Time > d.Time {
		return o
	}
	valueCompare := bytes.Compare(o.Value, d.Value)
	if (o.Time == d.Time && valueCompare == -1) || (o.Time == d.Time && valueCompare == 0 && d.Type < o.Type) {
		return o
	}
	return d
}

type (
	MembershipMap map[ids.ID]*Membership
	IdentityMap   map[ids.ID]MembershipMap
)

func (a IdentityMap) merge(b IdentityMap) (IdentityMap, error) {
	newMap := make(IdentityMap)
	for identID, ident := range a {
		newMap[identID] = make(MembershipMap)
		for membershipID, membership := range ident {
			if len(membership.Signature) != 0 {
				sigOk, err := verifyMembershipDesc([64]byte(membership.Signature), identID, membershipID, membership.Description)
				if err != nil {
					return nil, err
				}
				if !sigOk {
					return nil, fmt.Errorf("signature could not be verified")
				}
			} else if len(membership.Description.Endpoints) != 0 {
				return nil, fmt.Errorf("endpoints must be empty when signature is 0 length")
			}

			newMap[identID][membershipID] = &Membership{
				Description: &MembershipDescription{
					Version:   membership.Description.Version,
					Protocol:  membership.Description.Protocol,
					Endpoints: make(map[string]*EndpointInfo),
					IntroKey:  membership.Description.IntroKey,
				},
				Signature: membership.Signature,
			}
			for url, ei := range membership.Description.Endpoints {
				newMap[identID][membershipID].Description.Endpoints[url] = &EndpointInfo{Priority: ei.Priority}
			}
		}
	}
	for identID, ident := range b {
		if _, ok := newMap[identID]; !ok {
			newMap[identID] = make(MembershipMap)
		}
		for membershipID := range ident {
			membership := ident[membershipID]
			if len(membership.Signature) != 0 {
				sigOk, err := verifyMembershipDesc([64]byte(membership.Signature), identID, membershipID, membership.Description)
				if err != nil {
					return nil, err
				}
				if !sigOk {
					return nil, fmt.Errorf("signature could not be verified")
				}
			} else if len(membership.Description.Endpoints) != 0 {
				return nil, fmt.Errorf("endpoints must be empty when signature is 0 length")
			}

			if _, ok := newMap[identID][membershipID]; !ok {
				newMap[identID][membershipID] = membership
				continue
			}

			if membership.Description.IntroKey != newMap[identID][membershipID].Description.IntroKey {
				return nil, fmt.Errorf("intro keys must be the same")
			}

			newMem := newMap[identID][membershipID]
			if membership.Description.Version > newMem.Description.Version {
				newMem = membership
			} else if membership.Description.Version == newMem.Description.Version {
				aMembership := newMap[identID][membershipID]
				cmp, err := bencode.Compare(&aMembership, &membership)
				if err != nil {
					return nil, err
				}
				if cmp == 1 {
					newMap[identID][membershipID] = membership
				}
			}
			newMap[identID][membershipID] = newMem
		}
	}
	return newMap, nil
}

type GroupDescription struct {
	Name        *LWWString  `bencode:"n"`
	Description *LWWString  `bencode:"d"`
	Icon        *LWWBlob    `bencode:"ic"`
	IconType    *LWWString  `bencode:"it"`
	Identities  IdentityMap `bencode:"i"`
}

func (d *GroupDescription) encode() (b []byte, digest [32]byte, err error) {
	b, err = bencode.Serialize(d)
	if err != nil {
		err = fmt.Errorf("messaging: %w", err)
		return
	}
	digest = sha256.Sum256(b)
	return
}

func (d GroupDescription) merge(o *GroupDescription) (*GroupDescription, error) {
	mergedIdentities, err := d.Identities.merge(o.Identities)
	if err != nil {
		return nil, err
	}

	return &GroupDescription{
		Name:        d.Name.merge(o.Name),
		Description: d.Description.merge(o.Description),
		Icon:        d.Icon.merge(o.Icon),
		IconType:    d.IconType.merge(o.IconType),
		Identities:  mergedIdentities,
	}, nil
}

type GroupAcks struct {
	Acks map[ids.ID]map[ids.ID]*Ack `bencode:"a"`
}

type Ack struct {
	LastSeqAck    uint64 `bencode:"s"`
	LastSparseAck []byte `bencode:"sp"`
}

type Membership struct {
	Description *MembershipDescription `bencode:"d"`
	Signature   []byte                 `bencode:"s"`
}

type MembershipDescription struct {
	IntroKey  [32]byte                 `bencode:"k"`
	Version   uint32                   `bencode:"v"`
	Protocol  uint32                   `bencode:"p"`
	Endpoints map[string]*EndpointInfo `bencode:"es"`
}

type EndpointInfo struct {
	Priority uint8 `bencode:"p"`
}

type PrivateMessage struct {
	Type uint8  `bencode:"t"`
	Body []byte `bencode:"b"`
	Seq  uint64 `bencode:"s"`
}

type RepairBody struct {
	IdentityID   ids.ID `bencode:"i"`
	MembershipID ids.ID `bencode:"m"`
	Seq          uint64 `bencode:"s"`
	Body         []byte `bencode:"b"`
}

type BackfillRequest struct {
	ID   ids.ID `bencode:"i"`
	Type uint8  `bencode:"t"`
}

type BackfillStart struct {
	ID   ids.ID    `bencode:"i"`
	Acks GroupAcks `bencode:"a"`
}

type Backfill struct {
	ID    ids.ID `bencode:"i"`
	Total uint64 `bencode:"t"`
	Body  []byte `bencode:"b"`
}

type BackfillComplete struct {
	ID    ids.ID `bencode:"i"`
	Total uint64 `bencode:"t"`
}

type BackfillAbort struct {
	ID ids.ID `bencode:"i"`
}

type LostBody struct {
	Type uint8  `bencode:"t"`
	Body []byte `bencode:"b"`
}
