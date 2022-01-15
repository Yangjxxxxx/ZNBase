// Copyright 2021 The znbase Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings"
)

// FeatureTLSRequestNodeCertEnabled is used to enable and disable the user who has "admin" role
// request node user cert and primary key feature.
var FeatureTLSRequestNodeCertEnabled = settings.RegisterBoolSetting(
	"feature.request.nodecert.enabled",
	"set to true to enable the user who has \"admin\" role request node user cert and primary key, false to disable; default is false",
	false,
)

// RequestPartnerCert makes it possible for a node to request the node-to-node CA certificate.
func (s *adminServer) RequestPartnerCert(
	ctx context.Context, req *serverpb.NodeCertRequest,
) (*serverpb.NodeCertResponse, error) {

	settings := s.server.ClusterSettings()
	if settings == nil {
		return nil, errors.AssertionFailedf("could not look up cluster settings")
	}
	if !FeatureTLSRequestNodeCertEnabled.Get(&settings.SV) {
		return nil, errors.New("feature request node cert and primary key disabled by administrator")
	}

	//username, err := s.requireAdminUser(ctx)
	//if err != nil || "" == username {
	//	return nil, fmt.Errorf("RequestPartnerCert service requires user %s has admin role", username)
	//}
	if s.server.cfg.Insecure {
		return nil, fmt.Errorf("znbase server is insecure mode")
	}
	cm, err := s.server.rpcContext.GetCertificateManager()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get certificate manager")
	}

	list, err := cm.ListCertificates()
	for _, cert := range list {
		if security.NodePem == cert.FileUsage {
			key, err := security.PEMToPrivateKey(cert.KeyFileContents)
			if err != nil {
				return nil, errors.Errorf("error get node key: %s", err)
			}
			keyBytes, err := security.PrivateKeyToPKCS8(key)
			if err != nil {
				return nil, errors.Errorf("error get node key to PKCS8: %s", err)
			}
			return &serverpb.NodeCertResponse{
				NodeCert: string(cert.FileContents),
				NodeKey:  base64.StdEncoding.EncodeToString(keyBytes),
			}, nil
		}
	}

	return nil, errors.New("not found user node's key and cert")
}

//func (s *adminServer) requireAdminUser(ctx context.Context) (userName string, err error) {
//	userName, isAdmin, err := s.getUserAndRole(ctx)
//	if err != nil {
//		return "", err
//	}
//	if !isAdmin {
//		return "", errInsufficientPrivilege
//	}
//	return userName, nil
//}

//func (s *adminServer) getUserAndRole(
//	ctx context.Context,
//) (userName string, isAdmin bool, err error) {
//	userName, err = userFromContext(ctx)
//	if err != nil {
//		return "", false, err
//	}
//	isAdmin, err = s.hasAdminRole(ctx, userName)
//	return userName, isAdmin, err
//}

//func (s *adminServer) hasAdminRole(ctx context.Context, sessionUser string) (bool, error) {
//	if sessionUser == security.RootUser {
//		// Shortcut.
//		return true, nil
//	}
//	rows, cols, err := s.server.internalExecutor.QueryWithUser(
//		ctx, "check-is-admin",
//		nil /* txn */, sessionUser, "SELECT zbdb_internal.is_admin()")
//	if err != nil {
//		return false, err
//	}
//	if len(rows) != 1 || len(cols) != 1 {
//		return false, fmt.Errorf("hasAdminRole: expected 1 row, got %d", len(rows))
//	}
//	dbDatum, ok := tree.AsDBool(rows[0][0])
//	if !ok {
//		return false, fmt.Errorf("hasAdminRole: expected bool, got %T", rows[0][0])
//	}
//	return bool(dbDatum), nil
//}

//var errInsufficientPrivilege = errors.New("this operation requires admin privilege")
