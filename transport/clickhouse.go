package transport

import (
	"flag"
	"fmt"
	"sync"

	"database/sql"
	"github.com/ClickHouse/clickhouse-go"
	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/utils"
)

var (
	ClickHouseAddr     *string
	ClickHousePort     *int
	ClickHouseUser     *string
	ClickHousePassword *string
	ClickHouseDatabase *string
	count              uint64
	tx                 *sql.Tx

	dbConn *sql.DB
)

type ClickHouseState struct {
	logger utils.Logger

	FixedLengthProto bool
}

func RegisterFlags() {
	ClickHouseAddr = flag.String("ch.addr", "127.0.0.1", "ClickHouse DB Host")
	ClickHousePort = flag.Int("ch.port", 9000, "ClickHouse DB port")
	ClickHouseUser = flag.String("ch.username", "default", "ClickHouse username")
	ClickHousePassword = flag.String("ch.password", "default", "ClickHouse password")
	ClickHouseDatabase = flag.String("ch.database", "default", "ClickHouse database")

	// future: add batch size to batch insert
}

func StartClickHouseConnection(logger utils.Logger, withConnDebugLog bool) (*ClickHouseState, error) {

	count = 0

	if ClickHouseAddr == nil {
		temp := "<nil>"        // *string cannot be initialized
		ClickHouseAddr = &temp // in one statement
	}

	logger.Infof("clickhouse server on %v:%v", *ClickHouseAddr, *ClickHousePort)

	connStr := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s",
		*ClickHouseAddr, *ClickHousePort, *ClickHouseUser, *ClickHousePassword, *ClickHouseDatabase)

	if withConnDebugLog {
		connStr += "&debug=true"
	}

	// open DB dbConnion stuff
	connect, err := sql.Open("clickhouse", connStr)
	dbConn = connect
	if err != nil {
		logger.Fatalf("couldn't dbConn to db (%v)", err)
	}
	if err := dbConn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Errorf("[%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace)
		} else {
			logger.Error(err)
		}
		// return
	}

	// create DB schema, if not exist
	_, err = dbConn.Exec(fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS %s
	`, *ClickHouseDatabase))
	if err != nil {
		logger.Fatalf("couldn't create database '%s' (%v)", *ClickHouseDatabase, err)
	}

	// use MergeTree engine to optimize storage
	//https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/
	_, err = dbConn.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.nflow (
    
	    TimeReceived UInt32,
	    TimeFlowStart UInt32,
	    TimeFlowEnd UInt32,
	    Bytes UInt16,
	    Etype UInt32,
	    Packets UInt64,
	    SrcAddr UInt32,
	    DstAddr UInt32,
	    SrcPort UInt32,
	    DstPort UInt32,
	    Proto UInt32,
	    SrcMac UInt64,
	    DstMac UInt64,
	    SrcVlan UInt32,
	    DstVlan UInt32,
	    VlanId UInt32,
	    FlowType UInt8

	) ENGINE = MergeTree() 
	ORDER BY (TimeReceived, SrcAddr, SrcPort, DstAddr, DstPort)
	PARTITION BY DstAddr
	SAMPLE BY SrcAddr
	`, *ClickHouseDatabase))

	if err != nil {
		logger.Fatalf("couldn't create table (%v)", err)
	}

	// start transaction prep

	// defer stmt.Close()
	state := ClickHouseState{
		logger:           logger,
		FixedLengthProto: true,
	}

	return &state, nil

}

func ipv4BytesToUint32(b []byte) uint32 {
	return uint32(b[0])<<24 + uint32(b[1])<<16 + uint32(b[2])<<8 + uint32(b[3])
}

func (s *ClickHouseState) Insert(fm *flowmessage.FlowMessage, stmt *sql.Stmt, wg *sync.WaitGroup) {
	// extract fields out of the flow message

	// assume and encode as IPv4 (even if its v6)
	srcAddr := ipv4BytesToUint32(fm.GetSrcAddr()[:4])
	dstAddr := ipv4BytesToUint32(fm.GetDstAddr()[:4])

	count += 1
	// fmt.Printf("stmt: %v\n", stmt)
	if _, err := stmt.Exec(
		fm.GetTimeReceived(),
		fm.GetTimeFlowStart(),
		fm.GetTimeFlowEnd(),
		fm.GetBytes(),
		fm.GetEtype(),
		fm.GetPackets(),
		srcAddr,
		dstAddr,
		fm.GetSrcPort(),
		fm.GetDstPort(),
		fm.GetProto(),
		fm.GetSrcMac(),
		fm.GetDstMac(),
		fm.GetSrcVlan(),
		fm.GetDstVlan(),
		fm.GetVlanId(),
		uint8(fm.GetType()),
	); err != nil {
		s.logger.Errorf("error inserting record (%v)", err)
	}

	wg.Done()

	// -----------------------------------------------

	s.logger.Debugf("src (%v) %v:%v\ndst (%v) %v:%v\ncount:%v\n------\n",
		srcAddr,
		fm.GetSrcAddr(),
		fm.GetSrcPort(),
		dstAddr,
		fm.GetDstAddr(),
		fm.GetDstPort(),
		count)

}

func (s *ClickHouseState) Publish(msgs []*flowmessage.FlowMessage) {
	// ht: this is all you need to implement for the transport interface
	// needs to be async??

	// we need a semaphore / counter that increments inside the goroutines
	// WaitGroup ~= semaphore

	var wg sync.WaitGroup

	tx, _ = dbConn.Begin()

	stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO %s.nflow(TimeReceived, 
		TimeFlowStart,TimeFlowEnd,Bytes,Etype,Packets,SrcAddr,DstAddr,SrcPort,
		DstPort,Proto,SrcMac,DstMac,SrcVlan,DstVlan,VlanId,FlowType) 
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, *ClickHouseDatabase))

	if err != nil {
		s.logger.Errorf("Couldn't prepare statement (%v)", err)
		// stmt.Close()
		return
	}

	for _, msg := range msgs {
		wg.Add(1)
		go s.Insert(msg, stmt, &wg)
	}

	wg.Wait()
	defer stmt.Close()

	if err := tx.Commit(); err != nil {
		s.logger.Errorf("Couldn't commit transactions (%v)", err)
	}

	// commit after all of those are inserted
	// fmt.Println("\noutside loop!\n")

}
