package p

import (
        "context"
        "fmt"
        "log"
        "time"
        "bufio"
        "cloud.google.com/go/storage"
)

type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}
func PrintFileContent(ctx context.Context, e GCSEvent) error{
        client, err := storage.NewClient(ctx)
        if err != nil {
                log.Fatal(err)
        }
        read(client, e.Bucket, e.Name)
        return nil
}
func read(client *storage.Client, bucket, object string) {
        // [START download_file]
        ctx := context.Background()
        ctx, cancel := context.WithTimeout(ctx, time.Second*50)
        defer cancel()
        rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
        if err != nil {
                log.Fatal(err)
        }
        defer rc.Close()

        scanner := bufio.NewScanner(rc)
        for scanner.Scan() {
          fmt.Println(scanner.Text())
        }

        if err := scanner.Err(); err != nil {
          log.Fatal(err)
        }
        // [END download_file]
}
