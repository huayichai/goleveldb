package goleveldb

import "fmt"

func sstableFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/%06d.ldb", dbname, number)
}

func manifestFileName(dbname string) string {
	return fmt.Sprintf("%s/MANIFEST", dbname)
}

func walFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/%06d.log", dbname, number)
}
