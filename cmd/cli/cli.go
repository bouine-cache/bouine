/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package main

import (
	"context"
	"log"
	"time"

	"github.com/spf13/cobra"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	cacheKey string
)

func init() {
	rootCmd.AddCommand(purgeCmd)
	rootCmd.AddCommand(invalidateCmd)

	// Invalidate flags.
	invalidateCmd.Flags().StringVarP(&cacheKey, "key", "k", "", "the cache key to delete")
}

// purgeCmd represents the purge command.
var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "purge a cluster cache",
	Long: `Purge a cluster cache entirely.
	This will be applied on the FSM of the leader as well as any Candidate and Followers of the cluster.

	warning: this generates a heavy activity on the LSM trees of every bouine nodes.`,
	Run: func(cmd *cobra.Command, args []string) {
		var conn *grpc.ClientConn
		conn, err := grpc.Dial(raftAddress, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("did not connect: %s", err)
		}
		defer conn.Close()

		client := pb.NewCacheClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_, err = client.PurgeCache(ctx, &pb.PurgeCacheRequest{Since: timestamppb.New(time.Time{})})
		if err != nil {
			log.Fatalf("failed to purge bouine cache: %s", err)
		}
	},
}

// invalidateCmd represents the purge command.
var invalidateCmd = &cobra.Command{
	Use:   "invalidate",
	Short: "invalidate a cluster cache",
	Long: `Invalidate specific key from a cluster cache.
	This will be applied on the FSM of the leader as well as any Candidate and Followers of the cluster.`,
	Run: func(cmd *cobra.Command, args []string) {
		var conn *grpc.ClientConn
		conn, err := grpc.Dial(raftAddress, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("did not connect: %s", err)
		}
		defer conn.Close()

		client := pb.NewCacheClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_, err = client.InvalidateCacheEntry(ctx, &pb.InvalidateCacheEntryRequest{CacheKey: []byte(cacheKey)})
		if err != nil {
			log.Fatalf("failed to invalidate %s cache key: %s", cacheKey, err)
		}
	},
}
