package messaging

import (
	"errors"

	"github.com/joshbuddy/jpake"
	"github.com/meow-io/go-slick/ids"
)

var jpakeConfig = jpake.NewConfig().
	SetSessionConfirmationBytes([]byte("SLICK_KC")).
	SetSecretGenerationBytes([]byte("SLICK_SECRET")).
	SetSessionGenerationBytes([]byte("SLICK_SESSION"))

// use type aliases to avoid so much typing
type zkpMsgCurve25519 = jpake.ZKPMsg[*jpake.Curve25519Point, *jpake.Curve25519Scalar]
type threePassJpakeCurve25519 = jpake.ThreePassJpake[*jpake.Curve25519Point, *jpake.Curve25519Scalar]
type threePassVariant1Curve25519 = jpake.ThreePassVariant1[*jpake.Curve25519Point, *jpake.Curve25519Scalar]
type threePassVariant2Curve25519 = jpake.ThreePassVariant2[*jpake.Curve25519Point, *jpake.Curve25519Scalar]
type threePassVariant3Curve25519 = jpake.ThreePassVariant3[*jpake.Curve25519Point, *jpake.Curve25519Scalar]

var curve25519Curve = jpake.Curve25519Curve{}

func zkpToBencode(proof zkpMsgCurve25519) *ZKPBody {
	return &ZKPBody{
		T: proof.T.Bytes(),
		R: proof.R.Bytes(),
	}
}

func bencodeToZKP(b *ZKPBody) (*zkpMsgCurve25519, error) {
	tP, err := curve25519Curve.NewPoint().SetBytes(b.T)
	if err != nil {
		return &zkpMsgCurve25519{}, err
	}
	rS, err := curve25519Curve.NewScalar().SetBytes(b.R)
	if err != nil {
		return &zkpMsgCurve25519{}, err
	}
	return &zkpMsgCurve25519{
		T: tP,
		R: rS,
	}, nil
}

type jpakeState struct {
	ID          []byte `db:"id"`
	OtherX1G    []byte `db:"other_x1_g"`
	OtherX2G    []byte `db:"other_x2_g"`
	OtherUserID []byte `db:"other_user_id"`
	X1          []byte `db:"x1"`
	X2          []byte `db:"x2"`
	S           []byte `db:"s"`
	SessionKey  []byte `db:"sk"`
	ExternalID  []byte `db:"external_id"`
	Stage       int    `db:"stage"`

	jp *threePassJpakeCurve25519
}

func (j *jpakeState) updateFromJpake(jp *threePassJpakeCurve25519) {
	j.jp = jp
	j.OtherUserID = jp.OtherUserID

	if jp.OtherX1G != nil {
		j.OtherX1G = jp.OtherX1G.Bytes()
	} else {
		j.OtherX1G = nil
	}
	if jp.OtherX2G != nil {
		j.OtherX2G = jp.OtherX2G.Bytes()
	} else {
		j.OtherX2G = nil
	}
	j.X1 = jp.X1.Bytes()
	j.X2 = jp.X2.Bytes()
	j.S = jp.S.Bytes()
	j.Stage = jp.Stage
	if len(jp.SessionKey) != 0 {
		j.SessionKey = jp.SessionKey
	}
}

func (j *jpakeState) init() error {
	x1S, err := curve25519Curve.NewScalar().SetBytes(j.X1)
	if err != nil {
		return err
	}
	x2S, err := curve25519Curve.NewScalar().SetBytes(j.X2)
	if err != nil {
		return err
	}
	sS, err := curve25519Curve.NewScalar().SetBytes(j.S)
	if err != nil {
		return err
	}
	var otherX1GP, otherX2GP *jpake.Curve25519Point
	if j.OtherX1G != nil {
		otherX1GP, err = curve25519Curve.NewPoint().SetBytes(j.OtherX1G)
		if err != nil {
			return err
		}
	}
	if j.OtherX2G != nil {
		otherX2GP, err = curve25519Curve.NewPoint().SetBytes(j.OtherX2G)
		if err != nil {
			return err
		}
	}
	jp, err := jpake.RestoreThreePassJpakeWithConfig(
		j.Stage,
		j.ID,          // userID
		j.OtherUserID, // otherUserID
		j.SessionKey,  // sessionKey
		x1S,           // x1
		x2S,           // x2
		sS,            // s
		otherX1GP,     // otherX1G
		otherX2GP,     // otherX2G,
		jpakeConfig,
	)
	if err != nil {
		return err
	}
	j.jp = jp
	return nil
}

func (j *jpakeState) pass1Message() (*Jpake1, error) {
	jpakePass1, err := j.jp.Pass1Message()
	if err != nil {
		return nil, err
	}
	j.updateFromJpake(j.jp)
	return &Jpake1{
		ID:     [16]byte(j.ExternalID),
		UserID: ids.IDFromBytes(j.ID),
		X1G:    jpakePass1.X1G.Bytes(),
		X2G:    jpakePass1.X2G.Bytes(),
		X1ZKP:  zkpToBencode(jpakePass1.X1ZKP),
		X2ZKP:  zkpToBencode(jpakePass1.X2ZKP),
	}, nil
}

func (j *jpakeState) processPass1Message(pass1 *Jpake1) (_ *jpake2, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				if x == "edwards25519: use of uninitialized Point" {
					err = errors.New(x)
				}
			default:
				panic(r)
			}
		}
	}()
	var x1gP, x2gP *jpake.Curve25519Point
	var x1zkp, x2zkp *zkpMsgCurve25519
	x1gP, err = curve25519Curve.NewPoint().SetBytes(pass1.X1G)
	if err != nil {
		return nil, err
	}
	x2gP, err = curve25519Curve.NewPoint().SetBytes(pass1.X2G)
	if err != nil {
		return nil, err
	}

	x1zkp, err = bencodeToZKP(pass1.X1ZKP)
	if err != nil {
		return nil, err
	}
	x2zkp, err = bencodeToZKP(pass1.X2ZKP)
	if err != nil {
		return nil, err
	}

	jpakePass1 := threePassVariant1Curve25519{
		UserID: pass1.UserID[:],
		X1G:    x1gP,
		X2G:    x2gP,
		X1ZKP:  *x1zkp,
		X2ZKP:  *x2zkp,
	}

	jp2, err := j.jp.GetPass2Message(jpakePass1)
	if err != nil {
		return nil, err
	}

	j.updateFromJpake(j.jp)
	return &jpake2{
		ID:     pass1.ID,
		UserID: ids.IDFromBytes(j.ID),
		X3G:    jp2.X3G.Bytes(),
		X4G:    jp2.X4G.Bytes(),
		B:      jp2.B.Bytes(),
		X3ZKP:  zkpToBencode(jp2.X3ZKP),
		X4ZKP:  zkpToBencode(jp2.X4ZKP),
		XsZKP:  zkpToBencode(jp2.XsZKP),
	}, nil
}

func (j *jpakeState) processPass2Message(pass2 *jpake2) (_ *jpake3, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				if x == "edwards25519: use of uninitialized Point" {
					err = errors.New(x)
				}
			default:
				panic(r)
			}
		}
	}()
	var x3gP, x4gP, bP *jpake.Curve25519Point
	var x3zkp, x4zkp, xszkp *zkpMsgCurve25519
	x3gP, err = curve25519Curve.NewPoint().SetBytes(pass2.X3G)
	if err != nil {
		return nil, err
	}
	x4gP, err = curve25519Curve.NewPoint().SetBytes(pass2.X4G)
	if err != nil {
		return nil, err
	}
	bP, err = curve25519Curve.NewPoint().SetBytes(pass2.B)
	if err != nil {
		return nil, err
	}

	x3zkp, err = bencodeToZKP(pass2.X3ZKP)
	if err != nil {
		return nil, err
	}
	x4zkp, err = bencodeToZKP(pass2.X4ZKP)
	if err != nil {
		return nil, err
	}
	xszkp, err = bencodeToZKP(pass2.XsZKP)
	if err != nil {
		return nil, err
	}

	jpakePass2 := threePassVariant2Curve25519{
		UserID: pass2.UserID[:],
		X3G:    x3gP,
		X4G:    x4gP,
		B:      bP,
		X3ZKP:  *x3zkp,
		X4ZKP:  *x4zkp,
		XsZKP:  *xszkp,
	}

	jp3, err := j.jp.GetPass3Message(jpakePass2)
	if err != nil {
		return nil, err
	}

	j.updateFromJpake(j.jp)
	return &jpake3{
		ID:    pass2.ID,
		A:     jp3.A.Bytes(),
		XsZKP: zkpToBencode(jp3.XsZKP),
	}, nil
}

func (j *jpakeState) processPass3Message(pass3 *jpake3) (conf1 []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				if x == "edwards25519: use of uninitialized Point" {
					err = errors.New(x)
				}
			default:
				panic(r)
			}
		}
	}()
	var aP *jpake.Curve25519Point
	var xszkp *zkpMsgCurve25519
	aP, err = curve25519Curve.NewPoint().SetBytes(pass3.A)
	if err != nil {
		return
	}

	xszkp, err = bencodeToZKP(pass3.XsZKP)
	if err != nil {
		return
	}

	jpakePass3 := threePassVariant3Curve25519{
		A:     aP,
		XsZKP: *xszkp,
	}
	conf1, err = j.jp.ProcessPass3Message(jpakePass3)
	if err != nil {
		return
	}
	j.updateFromJpake(j.jp)
	return
}

func (j *jpakeState) processSessionConfirmation1(confirm1 []byte) ([]byte, error) {
	conf2, err := j.jp.ProcessSessionConfirmation1(confirm1)
	if err != nil {
		return nil, err
	}
	j.updateFromJpake(j.jp)
	return conf2, nil
}

func (j *jpakeState) processSessionConfirmation2(confirm2 []byte) error {
	if err := j.jp.ProcessSessionConfirmation2(confirm2); err != nil {
		return err
	}
	j.updateFromJpake(j.jp)
	return nil
}

func initJpake(initiator bool, externalID, secret []byte) (*jpakeState, error) {
	id := ids.NewID()
	jp, err := jpake.InitThreePassJpakeWithConfig(
		initiator,
		id[:],
		secret,
		jpakeConfig,
	)
	if err != nil {
		return nil, err
	}
	j := jpakeState{
		ID:         id[:],
		ExternalID: externalID,
	}
	j.updateFromJpake(jp)
	return &j, nil
}
