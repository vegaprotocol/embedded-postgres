package embeddedpostgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// EmbeddedPostgres maintains all configuration and runtime functions for maintaining the lifecycle of one Postgres process.
type EmbeddedPostgres struct {
	config              Config
	cacheLocator        CacheLocator
	remoteFetchStrategy RemoteFetchStrategy
	initDatabase        initDatabase
	createDatabase      createDatabase
	started             bool
	logger              PostgresLogger
	postgresProcess     *exec.Cmd
	cancelProcess       context.CancelFunc
}

type PostgresLogger interface {
	flush() error
	stdOut() io.Writer
	stdErr() io.Writer
}

// NewDatabase creates a new EmbeddedPostgres struct that can be used to start and stop a Postgres process.
// When called with no parameters it will assume a default configuration state provided by the DefaultConfig method.
// When called with parameters the first Config parameter will be used for configuration.
func NewDatabase(config ...Config) *EmbeddedPostgres {
	if len(config) < 1 {
		return newDatabaseWithConfig(DefaultConfig())
	}

	return newDatabaseWithConfig(config[0])
}

func newDatabaseWithConfig(config Config) *EmbeddedPostgres {
	versionStrategy := defaultVersionStrategy(
		config,
		runtime.GOOS,
		runtime.GOARCH,
		linuxMachineName,
		shouldUseAlpineLinuxBuild,
	)
	cacheLocator := defaultCacheLocator(versionStrategy)
	remoteFetchStrategy := defaultRemoteFetchStrategy("https://github.com", "vega-v0.1.0", versionStrategy, cacheLocator)

	return &EmbeddedPostgres{
		config:              config,
		cacheLocator:        cacheLocator,
		remoteFetchStrategy: remoteFetchStrategy,
		initDatabase:        defaultInitDatabase,
		createDatabase:      defaultCreateDatabase,
		started:             false,
	}
}

// Start will try to start the configured Postgres process returning an error when there were any problems with invocation.
// If any error occurs Start will try to also Stop the Postgres process in order to not leave any sub-process running.
//
//nolint:funlen
func (ep *EmbeddedPostgres) Start() error {
	if ep.started {
		return errors.New("server is already started")
	}

	if ep.config.listenAddr != "" {
		if err := ensurePortAvailable(ep.config.port); err != nil {
			return err
		}
	}

	var logger PostgresLogger
	if _, err := os.Stat(os.TempDir()); err == nil {
		logger, err = newSyncedLogger("", ep.config.logger)
		if err != nil {
			return fmt.Errorf("unable to create logger:%w", err)
		}
	} else {
		logger = newDefaultLogger()
	}
	ep.logger = logger

	cacheLocation, cacheExists := ep.cacheLocator()

	if ep.config.runtimePath == "" {
		ep.config.runtimePath = filepath.Join(filepath.Dir(cacheLocation), "extracted")
	}

	if ep.config.dataPath == "" {
		ep.config.dataPath = filepath.Join(ep.config.runtimePath, "data")
	}

	if err := os.RemoveAll(ep.config.runtimePath); err != nil {
		return fmt.Errorf("unable to clean up runtime directory %s with error: %w", ep.config.runtimePath, err)
	}

	if ep.config.binariesPath == "" {
		ep.config.binariesPath = ep.config.runtimePath
	}

	_, binDirErr := os.Stat(filepath.Join(ep.config.binariesPath, "bin"))
	if os.IsNotExist(binDirErr) {
		if !cacheExists {
			if err := ep.remoteFetchStrategy(); err != nil {
				return err
			}
		}

		if err := decompressTarXz(defaultTarReader, cacheLocation, ep.config.binariesPath); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(ep.config.runtimePath, 0o755); err != nil {
		return fmt.Errorf("unable to create runtime directory %s with error: %w", ep.config.runtimePath, err)
	}

	reuseData := dataDirIsValid(ep.config.dataPath, ep.config.version)

	if !reuseData {
		if err := ep.cleanDataDirectoryAndInit(); err != nil {
			return err
		}
	}

	if err := startPostgres(ep); err != nil {
		return err
	}

	if err := ep.logger.flush(); err != nil {
		return err
	}

	ep.started = true

	if !reuseData {
		op := func() error {
			host := ep.config.listenAddr
			if host == "" {
				host = ep.config.socketDir
			}
			return ep.createDatabase(host, ep.config.port, ep.config.username, ep.config.password, ep.config.database)
		}

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.MaxElapsedTime = 10 * time.Second

		if err := backoff.Retry(op, expBackoff); err != nil {
			if stopErr := stopPostgres(ep); stopErr != nil {
				return fmt.Errorf("unable to stop database casused by error %w", err)
			}

			return err
		}
	}

	if err := healthCheckDatabaseOrTimeout(ep.config); err != nil {
		if stopErr := stopPostgres(ep); stopErr != nil {
			return fmt.Errorf("unable to stop database casused by error %w", err)
		}

		return err
	}

	return nil
}

func (ep *EmbeddedPostgres) cleanDataDirectoryAndInit() error {
	if err := os.RemoveAll(ep.config.dataPath); err != nil {
		return fmt.Errorf("unable to clean up data directory %s with error: %w", ep.config.dataPath, err)
	}

	if err := ep.initDatabase(ep.config.binariesPath, ep.config.runtimePath, ep.config.dataPath, ep.config.username, ep.config.password, ep.config.locale, ep.logger); err != nil {
		return err
	}

	return nil
}

// Stop will try to stop the Postgres process gracefully returning an error when there were any problems.
func (ep *EmbeddedPostgres) Stop() error {
	if !ep.started {
		return errors.New("server has not been started")
	}

	if err := stopPostgres(ep); err != nil {
		return err
	}

	ep.started = false

	if err := ep.logger.flush(); err != nil {
		return err
	}

	return nil
}

func startPostgres(ep *EmbeddedPostgres) error {
	postgresBinary := filepath.Join(ep.config.binariesPath, "bin/postgres")

	if ep.postgresProcess != nil && ep.postgresProcess.Process != nil {
		if err := stopPostgres(ep); err != nil {
			return err
		}
	}

	ep.postgresProcess = exec.Command(postgresBinary,
		"-D", ep.config.dataPath,
		"-p", fmt.Sprintf("%d", ep.config.port),
		"-h", ep.config.listenAddr,
		"-c", fmt.Sprintf("unix_socket_directories=%s", ep.config.socketDir))
	ep.postgresProcess.Stdout = ep.logger.stdOut()
	ep.postgresProcess.Stderr = ep.logger.stdErr()

	var postgresStartErr error

	timeout, cancel := context.WithTimeout(context.Background(), time.Second*2)

	defer cancel()

	postgresStartErr = ep.postgresProcess.Start()

	for {
		select {
		case <-timeout.Done():
			return postgresStartErr
		default:
			if err := ensureConnectionAvailable(ep); err != nil {
				// the port is open so assume postgres has started
				return nil
			}

			if postgresStartErr != nil {
				return postgresStartErr
			}
		}
	}
}

func stopPostgres(ep *EmbeddedPostgres) error {
	if ep.postgresProcess != nil && ep.postgresProcess.Process != nil {
		if err := ep.postgresProcess.Process.Signal(syscall.SIGINT); err != nil {
			return err
		}

		op := func() error {
			return ensureConnectionAvailable(ep)
		}

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.MaxElapsedTime = time.Second * 5

		if err := backoff.Retry(op, expBackoff); err != nil {
			return fmt.Errorf("could not stop Postgres server: %w", err)
		}
	}

	ep.postgresProcess = nil

	return nil
}

func ensureConnectionAvailable(ep *EmbeddedPostgres) error {
	if ep.config.listenAddr == "" {
		return ensureUnixSocketAvailable(fmt.Sprintf("%s/.s.PGSQL.%d", ep.config.socketDir, ep.config.port))
	}
	return ensurePortAvailable(ep.config.port)
}

func ensureUnixSocketAvailable(path string) error {
	conn, err := net.DialTimeout("unix", path, 100*time.Millisecond)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		// If we couldn't connect, that's great - probably nothing is listening there
		return nil
	}
	return fmt.Errorf("Something listening on unix socket %v", path)
}

func ensurePortAvailable(port uint32) error {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprintf("%d", port)), 100*time.Millisecond)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		// If we couldn't connect, that's great - probably nothing is listening there
		return nil
	}
	return fmt.Errorf("Something listening on port %v", port)
}

func dataDirIsValid(dataDir string, version PostgresVersion) bool {
	pgVersion := filepath.Join(dataDir, "PG_VERSION")

	d, err := ioutil.ReadFile(pgVersion)
	if err != nil {
		return false
	}

	v := strings.TrimSuffix(string(d), "\n")

	return strings.HasPrefix(string(version), v)
}
