package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDependencyBoundaries(t *testing.T) {
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, spec := range file.Imports {
			name, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			lower := strings.ToLower(name)
			for _, banned := range []string{"github.com/fangcunmount/iam", "github.com/fangcunmount/qs-server", "github.com/fangcunmount/qs-ai", "github.com/fangcunmount/component-base"} {
				if lower == banned || strings.HasPrefix(lower, banned+"/") {
					t.Errorf("%s imports forbidden host dependency %s", rel, name)
				}
			}
			driver := strings.HasPrefix(name, "gorm.io/") || strings.HasPrefix(name, "go.mongodb.org/") || strings.HasPrefix(name, "github.com/go-sql-driver/") || strings.HasPrefix(name, "github.com/nsqio/") || strings.HasPrefix(name, "github.com/rabbitmq/")
			adapter := strings.HasPrefix(rel, "storage/") || strings.HasPrefix(rel, "transport/nsq/") || strings.HasPrefix(rel, "transport/rabbitmq/") || strings.HasPrefix(rel, "tests/integration/") || strings.HasPrefix(rel, "examples/")
			if driver && !adapter {
				t.Errorf("%s imports adapter driver %s", rel, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
