package build

import (
	"os"
	"os/exec"
)

func RsyncDB(src, dst string) error {
	cmd := exec.Command("rsync", "-av", "--progress", src, dst)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
