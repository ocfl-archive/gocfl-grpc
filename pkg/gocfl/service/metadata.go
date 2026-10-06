package service

import (
	"encoding/json"
	"time"

	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/inventory"
)

// MetadataToProto converts an inventory.Metadata instance into its Protobuf, JSON, and human-readable string representations.
func MetadataToProto(meta *inventory.Metadata, format string, obfuscate bool) (*pb.ObjectMetadataProto, string, string, error) {
	if meta == nil {
		return nil, "", "", nil
	}

	if obfuscate {
		if err := meta.Obfuscate(); err != nil {
			return nil, "", "", err
		}
	}

	jsonBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, "", "", err
	}
	jsonData := string(jsonBytes)
	humanData := meta.String()

	pbMeta := &pb.ObjectMetadataProto{
		Id:              meta.ID,
		DigestAlgorithm: string(meta.DigestAlgorithm),
		Versions:        make(map[string]*pb.VersionMetadataProto),
		Files:           make(map[string]*pb.FileMetadataProto),
	}

	if meta.Head != nil {
		pbMeta.Head = meta.Head.String()
	}

	if meta.Extension != nil {
		if extBytes, err := json.Marshal(meta.Extension); err == nil {
			pbMeta.ExtensionJson = string(extBytes)
		}
	}

	for verKey, v := range meta.Versions {
		if v == nil {
			continue
		}
		pbMeta.Versions[verKey] = &pb.VersionMetadataProto{
			Created: v.Created.Format(time.RFC3339),
			Message: v.Message,
			Name:    v.Name,
			Address: v.Address,
		}
	}

	for fileKey, f := range meta.Files {
		if f == nil {
			continue
		}
		fileProto := &pb.FileMetadataProto{
			Checksums:    make(map[string]string),
			InternalName: f.InternalName,
			VersionName:  make(map[string]*pb.VersionNamesProto),
		}
		for alg, sum := range f.Checksums {
			fileProto.Checksums[string(alg)] = sum
		}
		for ver, names := range f.VersionName {
			fileProto.VersionName[ver] = &pb.VersionNamesProto{
				Names: names,
			}
		}
		if f.Extension != nil {
			if extBytes, err := json.Marshal(f.Extension); err == nil {
				fileProto.ExtensionJson = string(extBytes)
			}
		}
		pbMeta.Files[fileKey] = fileProto
	}

	return pbMeta, jsonData, humanData, nil
}
