package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile              = configFile("ca.pem")
	ServerCertFile      = configFile("server.pem")
	ServerKeyFile       = configFile("server-key.pem")
	ClientCertFile      = configFile("client.pem")
	ClientKeyFile       = configFile("client-key.pem")
	FalseClientCertFile = configFile("false-client.pem")
	FalseClientKeyFile  = configFile("false-client-key.pem")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}

	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(homedir, ".commitlog", filename)
}
