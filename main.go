package main

import (
	"nginx-clickhouse/pkg/process"

	"github.com/erikdubbelboer/gspt"
)

const programName = "nginx-clickhouse"

func main() {
	gspt.SetProcTitle(programName)

	process.Work()
}
