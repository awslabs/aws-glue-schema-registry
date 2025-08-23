package integration_tests

import (
	"log"
)

type MemoryLeakSuite struct {
	ProtobufIntegrationSuite
}

func (m *MemoryLeakSuite) TestMemoryLeak() {
	log.Println("Running memory leak test (not implemented)")
	/*
		f, err := os.Create("mem.prof")
		if err != nil {
			m.T().Fatal(err)
		}
		defer f.Close()

		for i := 0; i < 1000; i++ {
			m.TestProtobufKafkaIntegration()
		}

		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	*/

}
