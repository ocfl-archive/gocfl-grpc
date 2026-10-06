package service

import (
	"time"

	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/inventory"
)

// InventoryToProto converts an OCFL inventory.Inventory domain model to a Protobuf Inventory message.
func InventoryToProto(inv inventory.Inventory) *pb.Inventory {
	if inv == nil {
		return nil
	}

	pbInv := &pb.Inventory{
		Id:               inv.GetID(),
		Type:             string(inv.GetSpec()),
		DigestAlgorithm:  string(inv.GetDigestAlgorithm()),
		ContentDirectory: inv.GetContentDir(),
	}

	if head := inv.GetHead(); head != nil {
		pbInv.Head = head.String()
	}

	// Manifest
	if manifest := inv.GetManifest(); manifest != nil {
		pbInv.Manifest = make(map[string]*pb.DigestPaths)
		for digest, paths := range manifest.Iterate() {
			pbInv.Manifest[digest] = &pb.DigestPaths{
				Paths: append([]string(nil), paths...),
			}
		}
	}

	// Versions
	if versions := inv.GetVersions(); versions != nil {
		pbInv.Versions = make(map[string]*pb.Version)
		for verNum, ver := range versions.Iterate() {
			if verNum == nil || ver == nil {
				continue
			}

			pbVer := &pb.Version{
				Created: ver.GetCreated().Format(time.RFC3339),
				Message: ver.GetMessage(),
			}

			if u := ver.GetUser(); u != nil {
				pbVer.User = &pb.User{
					Name:    u.GetName(),
					Address: u.GetAddress(),
				}
			}

			if state := ver.GetState(); state != nil {
				pbVer.State = make(map[string]*pb.DigestPaths)
				for digest, paths := range state.Iterate() {
					pbVer.State[digest] = &pb.DigestPaths{
						Paths: append([]string(nil), paths...),
					}
				}
			}

			pbInv.Versions[verNum.String()] = pbVer
		}
	}

	// Fixity
	if fixity := inv.GetFixity(); fixity != nil {
		pbInv.Fixity = make(map[string]*pb.FixityMap)
		for alg := range fixity.GetDigestAlgorithms() {
			fm := &pb.FixityMap{
				Digests: make(map[string]*pb.DigestPaths),
			}
			for digest, paths := range fixity.Iterate(alg) {
				fm.Digests[digest] = &pb.DigestPaths{
					Paths: append([]string(nil), paths...),
				}
			}
			pbInv.Fixity[string(alg)] = fm
		}
	}

	return pbInv
}
