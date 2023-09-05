package goleveldb

import "fmt"

func sstableFileName(dbpath string, number uint64) string {
	return fmt.Sprintf("%s/%06d.ldb", dbpath, number)
}

func manifestFileName(dbpath string) string {
	return fmt.Sprintf("%s/MANIFEST", dbpath)
}

func walFileName(dbpath string, number uint64) string {
	return fmt.Sprintf("%s/%06d.log", dbpath, number)
}
