<?php

namespace Echo511\CodeceptionModuleDatabase;

use Codeception\Configuration;
use Codeception\Exception\ModuleConfigException;
use Codeception\Exception\ModuleException;
use Codeception\Module\Db as DB_MODULE;
use Codeception\TestInterface;
use Codeception\Util\Debug;
use FluentPDO;
use InvalidArgumentException;
use PDOException;
use Phinx\Config\Config;
use Phinx\Console\Command\Migrate;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * MySQL support only.
 */
class Database extends DB_MODULE
{

    /**
     * @var array
     */
    protected $config = [
        'populate' => false,
        'cleanup' => false,
        'reconnect' => false,
        'dump' => null,
        'resetBeforeSuite' => true,
        'truncate' => false,
        'migrate' => false,
        'migrations_path' => null,
        'dbname' => null,
        'native' => false,
    ];

    /**
     * @var FluentPDO
     */
    protected $fluentPdo;


    public function _initialize()
    {
        parent::_initialize();
        if (!$this->config['resetBeforeSuite']) {
            $this->tryMigrate();
        }
    }


    protected function tryMigrate()
    {
        if ($this->config['migrate']) {
            if ($this->config['migrations_path'] === null) {
                throw new InvalidArgumentException('Please provide migrations_path in your configuration.');
            }
            if ($this->config['dbname'] === null) {
                throw new InvalidArgumentException('Please provide dbname in your configuration for phinx.');
            }
            $this->migrateDatabase();
        }
    }


    protected function tryTruncate()
    {
        if ($this->config['truncate']) {
            $this->debug('Database: truncate');
            $this->truncateDatabase();
        }
    }


    protected function reset()
    {
        $this->debug('Database: cleanup');
        $this->driver->cleanup();
        $this->populated = false;
        $this->useDatabaseDump($this->config['dump']);
        $this->tryMigrate();
        $this->tryTruncate();
    }


    public function _beforeSuite($settings = [])
    {
        if ($this->config['resetBeforeSuite']) {
            $this->reset();
        }
    }


    public function _before(TestInterface $test)
    {
        $this->tryTruncate();
    }


    /**
     * Return instance of fluentPDO.
     * @return FluentPDO
     */
    public function fluentDatabase()
    {
        if (!$this->fluentPdo) {
            $pdo = $this->dbh;
            $this->fluentPdo = new FluentPDO($pdo);
        }

        return $this->fluentPdo;
    }


    /**
     * Execute custom query.
     * @param string $query
     */
    public function queryDatabase($query)
    {
        $this->dbh->exec($query);
    }


    /**
     * Import to database.
     * @param string $sqlDump
     */
    public function useDatabaseDump($sqlDump)
    {
        if (!file_exists(Configuration::projectDir() . $sqlDump)) {
            throw new ModuleConfigException(
                __CLASS__,
                "\nFile with dump doesn't exist.\n"
                . "Please, check path for sql file: "
                . $sqlDump
            );
        }

        if ($this->config['native']) {
            $file = Configuration::projectDir() . $sqlDump;
            $command = 'mysql -h ' . $this->config['host'] . ' -u ' . $this->config['user'] . ' --password=\'' . $this->config['password'] . '\' ' . $this->config['dbname'] . ' < ' . $file;
            Debug::debug($command);
            exec($command);
        } else {
            $sql = file_get_contents(Configuration::projectDir() . $sqlDump);

            // remove C-style comments (except MySQL directives)
            $sql = preg_replace('%/\*(?!!\d+).*?\*/%s', '', $sql);

            if (!empty($sql)) {
                // split SQL dump into lines
                $sql = preg_split('/\r\n|\n|\r/', $sql, -1, PREG_SPLIT_NO_EMPTY);
            }

            try {
                $this->driver->load($sql);
            } catch (PDOException $e) {
                throw new ModuleException(
                    __CLASS__,
                    $e->getMessage() . "\nSQL query being executed: " . $this->driver->sqlToRun
                );
            }
        }
    }


    public function truncateDatabase($table = null)
    {
        $this->dbh->exec('SET FOREIGN_KEY_CHECKS=0;');
        if ($table === null) {
            $res = $this->dbh->query("SHOW FULL TABLES WHERE TABLE_TYPE LIKE '%TABLE';")->fetchAll();
            foreach ($res as $row) {
                $this->dbh->exec('truncate table `' . $row[0] . '`');
            }
        } elseif (is_array($table)) {
            foreach ($table as $tbl) {
                $this->dbh->exec('truncate table `' . $tbl . '`');
            }
        } else {
            $this->dbh->exec('truncate table `' . $table . '`');
        }
        $this->dbh->exec('SET FOREIGN_KEY_CHECKS=1;');
    }


    /**
     * Migrate using phinx migrations.
     */
    protected function migrateDatabase()
    {
        $migrationsPath = Configuration::projectDir() . $this->config['migrations_path'];
        if (!is_dir($migrationsPath)) {
            throw new ModuleConfigException(
                __CLASS__,
                "\nMigrations directory doesn't exist.\n"
                . "Please, check path for migrations direcotry: "
                . $migrationsPath
            );
        }

        $config = [
            'paths' => [
                'migrations' => $migrationsPath,
            ],
            'environments' => [
                'default_database' => 'db',
                'db' => [
                    'name' => $this->config['dbname'],
                    'connection' => $this->dbh,
                ]
            ]
        ];
        $commandConfig = new Config($config);
        $migrateCommand = new Migrate();
        $migrateCommand->setName('migrate');
        $migrateCommand->setConfig($commandConfig);

        $console = new Application();
        $console->add($migrateCommand);

        $input = new ArrayInput(['command' => 'migrate']);
        $output = new BufferedOutput();
        $console->doRun($input, $output);
        $this->debug("DatabaseModule:\n" . $output->fetch());
    }
}
