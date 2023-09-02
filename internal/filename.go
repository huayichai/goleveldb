package internal

import "fmt"

func SSTableFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/%06d.ldb", dbname, number)
}

func ManifestFileName(dbname string) string {
	return fmt.Sprintf("%s/MANIFEST", dbname)
}

func LogFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/%06d.log", dbname, number)
}
