package bangfuse

import (
	"fmt"
	"strings"
	"syscall"
)

func debugOpenFlags(flags uint32) string {
	var parts []string

	accessMode := flags & syscall.O_ACCMODE
	switch accessMode {
	case syscall.O_RDONLY:
		parts = append(parts, "O_RDONLY")
	case syscall.O_WRONLY:
		parts = append(parts, "O_WRONLY")
	case syscall.O_RDWR:
		parts = append(parts, "O_RDWR")
	}

	flagNames := []struct {
		flag uint32
		name string
	}{
		{syscall.O_APPEND, "O_APPEND"},
		{syscall.O_ASYNC, "O_ASYNC"},
		{syscall.O_CLOEXEC, "O_CLOEXEC"},
		{syscall.O_CREAT, "O_CREAT"},
		{syscall.O_DIRECT, "O_DIRECT"},
		{syscall.O_DIRECTORY, "O_DIRECTORY"},
		{syscall.O_DSYNC, "O_DSYNC"},
		{syscall.O_EXCL, "O_EXCL"},
		{syscall.O_NOATIME, "O_NOATIME"},
		{syscall.O_NOCTTY, "O_NOCTTY"},
		{syscall.O_NOFOLLOW, "O_NOFOLLOW"},
		{syscall.O_NONBLOCK, "O_NONBLOCK"},
		{syscall.O_SYNC, "O_SYNC"},
		{syscall.O_TRUNC, "O_TRUNC"},
	}

	for _, f := range flagNames {
		if flags&f.flag != 0 {
			parts = append(parts, f.name)
		}
	}

	if len(parts) == 0 {
		return fmt.Sprintf("0x%x", flags)
	}
	return strings.Join(parts, "|")
}
