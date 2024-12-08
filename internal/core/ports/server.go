package ports

type Server interface {
	StartReplication(host, port string) error
	StopReplication()
	IsMaster() bool
	GetMasterInfo() (string, string) // returns host, port
}
