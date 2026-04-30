package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/cache"
	"github.com/neatflowcv/cephclient/internal/pkg/podman"
)

const parserBindingCount = 3

func Run() error {
	return RunWithArgs(context.Background(), os.Args[1:], os.Stdin, os.Stdout)
}

func RunWithArgs(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer) error {
	cliApp := newApp()

	parser, err := NewParser(
		cliApp,
		ctx,
		stdin,
		stdout,
	)
	if err != nil {
		return fmt.Errorf("create CLI parser: %w", err)
	}

	kctx, err := parser.Parse(args)
	if err != nil {
		return fmt.Errorf("parse CLI arguments: %w", err)
	}

	podmanClient, err := podman.NewClient(cliApp.Debug, os.Stderr)
	if err != nil {
		return fmt.Errorf("create podman client: %w", err)
	}

	cachePath, err := defaultCachePath()
	if err != nil {
		return err
	}

	cachedClient, err := cache.NewClient(ctx, podmanClient, cachePath)
	if err != nil {
		return fmt.Errorf("create cache client: %w", err)
	}

	service := flow.NewService(cachedClient)
	kctx.Bind(service)

	runErr := kctx.Run()
	closeErr := cachedClient.Close()

	if runErr != nil {
		return fmt.Errorf("run CLI command: %w", runErr)
	}

	if closeErr != nil {
		return fmt.Errorf("close cache client: %w", closeErr)
	}

	return nil
}

func defaultCachePath() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("get user cache dir: %w", err)
	}

	return filepath.Join(cacheDir, "cephclient", "cache.sqlite"), nil
}

func NewParser(
	cliApp *app,
	ctx context.Context,
	stdin io.Reader,
	stdout io.Writer,
	options ...kong.Option,
) (*kong.Kong, error) {
	parserOptions := make([]kong.Option, 0, parserBindingCount+len(options))
	parserOptions = append(parserOptions,
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.BindTo(stdin, (*io.Reader)(nil)),
		kong.BindTo(stdout, (*io.Writer)(nil)),
	)
	parserOptions = append(parserOptions, options...)

	parser, err := kong.New(cliApp, parserOptions...)
	if err != nil {
		return nil, fmt.Errorf("create kong parser: %w", err)
	}

	return parser, nil
}
