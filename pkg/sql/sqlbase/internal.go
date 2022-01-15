package sqlbase

// Unused now.
// InternalExecutorSessionDataOverride is used by the InternalExecutor interface
// to allow control over some of the session data.
//type InternalExecutorSessionDataOverride struct {
//	// User represents the user that the query will run under.
//	User string
//	// Database represents the default database for the query.
//	Database string
//	// ApplicationName represents the application that the query runs under.
//	ApplicationName string
//	// SearchPath represents the namespaces to search in.
//	SearchPath *sessiondata.SearchPath
//}

// NoSessionDataOverride is the empty InternalExecutorSessionDataOverride which
// does not override any session data.
//var NoSessionDataOverride = InternalExecutorSessionDataOverride{}

// NodeUserSessionDataOverride is an InternalExecutorSessionDataOverride which
// overrides the users to the NodeUser.
//var NodeUserSessionDataOverride = InternalExecutorSessionDataOverride{User: security.NodeUser}
