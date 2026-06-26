package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "foundational-foundational-store",
	Short: "A foundational foundational-store server and utilities",
	Long: `A foundational foundational-store server and utilities for managing and interacting with 
various storage backends including PostgreSQL, Badger, and more.`,
}

func init() {
	cobra.OnInitialize(initConfig)

	// Add global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.foundational-foundational-store.yaml)")

	// Add commands
	rootCmd.AddCommand(GetCmd)
	rootCmd.AddCommand(ServerCmd)
	rootCmd.AddCommand(RemoteFeedCmd)
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".foundational-foundational-store" (without extension)
		viper.AddConfigPath(home)
		viper.SetConfigName(".foundational-foundational-store")
	}

	// Read in environment variables that match
	viper.SetEnvPrefix("FOUNDATIONAL_STORE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
