package syncq

type TaskCfg struct {
	PartitionKey string
}

func (cfg *TaskCfg) IsWithPartition() bool {
	return cfg.PartitionKey != ""
}
