<?php

namespace Doctrine\Tests\DBAL\Functional;

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
use const CASE_LOWER;
use function array_change_key_case;
use function fopen;
use function str_repeat;
use function stream_get_contents;

/**
 * @group DBAL-6
 */
class BlobTest extends \Doctrine\Tests\DbalFunctionalTestCase
{
    protected function setUp()
    {
        parent::setUp();

        if ($this->_conn->getDriver() instanceof \Doctrine\DBAL\Driver\PDOSqlsrv\Driver) {
            $this->markTestSkipped('This test does not work on pdo_sqlsrv driver due to a bug. See: http://social.msdn.microsoft.com/Forums/sqlserver/en-US/5a755bdd-41e9-45cb-9166-c9da4475bb94/how-to-set-null-for-varbinarymax-using-bindvalue-using-pdosqlsrv?forum=sqldriverforphp');
        }

        try {
            /* @var $sm \Doctrine\DBAL\Schema\AbstractSchemaManager */
            $table = new \Doctrine\DBAL\Schema\Table("blob_table");
            $table->addColumn('id', 'integer');
            $table->addColumn('clobfield', 'text');
            $table->addColumn('blobfield', 'blob');
            $table->setPrimaryKey(array('id'));

            $sm = $this->_conn->getSchemaManager();
            $sm->createTable($table);
        } catch(\Exception $e) {

        }
        $this->_conn->exec($this->_conn->getDatabasePlatform()->getTruncateTableSQL('blob_table'));
    }

    public function testInsert()
    {
        $ret = $this->_conn->insert('blob_table', [
            'id'          => 1,
            'clobfield'   => 'test',
            'blobfield'   => 'test',
        ], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        self::assertEquals(1, $ret);
    }

    public function testInsertProcessesStream()
    {
        $this->_conn->insert('blob_table', [
            'id'          => 1,
            'clobfield'   => fopen('data://text/plain,test', 'r'),
            'blobfield'   => fopen('data://text/plain,test', 'r'),
        ], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        $this->assertClobContains('test');
        $this->assertBlobContains('test');
    }

    public function testInsertCanHandleStreamLongerThanChunkSize()
    {
        $longBlob = str_repeat('x', 40000);

        $this->_conn->insert('blob_table', [
            'id'          => 1,
            'clobfield'   => fopen('data://text/plain,' . $longBlob, 'r'),
            'blobfield'   => fopen('data://text/plain,' . $longBlob, 'r'),
        ], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        $this->assertClobContains($longBlob);
        $this->assertBlobContains($longBlob);
    }

    public function testSelect()
    {
        $this->_conn->insert('blob_table', [
            'id'          => 1,
            'clobfield'   => 'test',
            'blobfield'   => 'test',
        ], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        $this->assertClobContains('test');
        $this->assertBlobContains('test');
    }

    public function testUpdate()
    {
        $this->_conn->insert('blob_table', [
            'id' => 1,
            'clobfield' => 'test',
            'blobfield' => 'test',
        ], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        $this->_conn->update('blob_table', [
            'clobfield' => 'test2',
            'blobfield' => 'test2',
        ], ['id' => 1], [
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
            ParameterType::INTEGER,
        ]);

        $this->assertClobContains('test2');
        $this->assertBlobContains('test2');
    }

    public function testUpdateProcessesStream()
    {
        $this->_conn->insert('blob_table', [
            'id'          => 1,
            'clobfield'   => 'test',
            'blobfield'   => 'test',
        ], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        $this->_conn->update('blob_table', [
            'id'          => 1,
            'clobfield'   => fopen('data://text/plain,test2', 'r'),
            'blobfield'   => fopen('data://text/plain,test2', 'r'),
        ], ['id' => 1], [
            ParameterType::INTEGER,
            ParameterType::LARGE_OBJECT,
            ParameterType::LARGE_OBJECT,
        ]);

        $this->assertClobContains('test2');
        $this->assertBlobContains('test2');
    }

    private function assertClobContains($text)
    {
        $rows = $this->_conn->fetchAll('SELECT * FROM blob_table');

        self::assertCount(1, $rows);
        $row = array_change_key_case($rows[0], CASE_LOWER);

        $blobValue = Type::getType('text')->convertToPHPValue($row['clobfield'], $this->_conn->getDatabasePlatform());

        self::assertInternalType('string', $blobValue);
        self::assertEquals($text, $blobValue);
    }

    private function assertBlobContains($text)
    {
        $rows = $this->_conn->fetchAll('SELECT * FROM blob_table');

        self::assertCount(1, $rows);
        $row = array_change_key_case($rows[0], CASE_LOWER);

        $blobValue = Type::getType('blob')->convertToPHPValue($row['blobfield'], $this->_conn->getDatabasePlatform());

        self::assertInternalType('resource', $blobValue);
        self::assertEquals($text, stream_get_contents($blobValue));
    }
}
