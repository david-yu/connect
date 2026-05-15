// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/ebitengine/purego"
)

// DB2 CLI library handle
var (
	libHandle uintptr
	loadOnce  sync.Once
	loadErr   error
)

// getLibraryPath returns the platform-specific DB2 CLI library path
func getLibraryPath() string {
	switch runtime.GOOS {
	case "linux":
		// Try common DB2 installation paths on Linux
		return "libdb2.so.1" // Will search LD_LIBRARY_PATH
	case "darwin":
		return "libdb2.dylib"
	case "windows":
		return "db2cli.dll"
	default:
		return "libdb2.so"
	}
}

// LoadLibrary loads the DB2 CLI library using purego.
// The library is found via the system's dynamic linker search path (ldconfig cache,
// LD_LIBRARY_PATH, etc.). For tests that extract the library from a container,
// use LoadLibraryFromPath to specify the absolute path directly.
func LoadLibrary() error {
	return loadLibrary(getLibraryPath())
}

// LoadLibraryFromPath loads the DB2 CLI library from the given absolute path.
// This bypasses the linker cache search and is used by integration tests that
// extract libdb2.so.1 from the DB2 container into a known directory.
func LoadLibraryFromPath(path string) error {
	return loadLibrary(path)
}

func loadLibrary(libPath string) error {
	loadOnce.Do(func() {
		// RTLD_LOCAL (not RTLD_GLOBAL) prevents DB2 CLI symbols from leaking into
		// the global symbol namespace, which would allow a malicious libdb2.so on
		// LD_LIBRARY_PATH to hijack symbols used by other libraries.
		libHandle, loadErr = purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_LOCAL)
		if loadErr != nil {
			loadErr = fmt.Errorf("loading DB2 CLI library %s: %w (ensure DB2 client is installed and library is in PATH/LD_LIBRARY_PATH)", libPath, loadErr)
			return
		}

		loadErr = registerFunctions()
		if loadErr != nil {
			loadErr = fmt.Errorf("registering DB2 CLI functions: %w", loadErr)
		}
	})

	return loadErr
}

// registerFunctions registers all DB2 CLI function pointers
func registerFunctions() error {
	// This will register function pointers defined in functions.go
	// Each function will be registered individually using purego.RegisterLibFunc

	if err := registerConnectionFunctions(); err != nil {
		return fmt.Errorf("register connection functions: %w", err)
	}

	if err := registerStatementFunctions(); err != nil {
		return fmt.Errorf("register statement functions: %w", err)
	}

	if err := registerResultFunctions(); err != nil {
		return fmt.Errorf("register result functions: %w", err)
	}

	if err := registerTransactionFunctions(); err != nil {
		return fmt.Errorf("register transaction functions: %w", err)
	}

	if err := registerDiagnosticFunctions(); err != nil {
		return fmt.Errorf("register diagnostic functions: %w", err)
	}

	return nil
}

// IsLoaded returns true if the DB2 CLI library has been loaded
func IsLoaded() bool {
	return libHandle != 0 && loadErr == nil
}

// GetLibraryHandle returns the raw library handle (for advanced use cases)
func GetLibraryHandle() uintptr {
	return libHandle
}
