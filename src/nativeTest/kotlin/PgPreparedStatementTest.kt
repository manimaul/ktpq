import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class PgPreparedStatementTest {

    lateinit var ds: PgDataSource

    @BeforeTest
    fun beforeEach() {
        pgDebug = true
        ds = PgDataSource("postgresql://admin:mysecretpassword@localhost:5432/s57server", 1)
        assertEquals(1, ds.readyCount)
        runBlocking {
            ds.connection().use { conn ->
                conn.statement("drop table if exists testing;")
                    .execute()
                conn.statement("create table if not exists testing (id bigserial primary key, name varchar unique not null);")
                    .execute()
            }
        }
    }

    @AfterTest
    fun afterEach() {
        ds.close()
    }

    @Test
    fun testParamCount() {
        runBlocking {
            ds.connection().use { conn ->
                (conn.prepareStatement("SELECT name from testing LIMIT $1;") as PgStatement).also { statement ->
                    assertEquals(1, statement.parameters)
                }
                (conn.prepareStatement("SELECT name from testing WHERE name=$1 LIMIT $2;") as PgStatement).also { statement ->
                    assertEquals(2, statement.parameters)
                }
            }
        }
    }

    @Test
    fun testPreparedStatementExists() {
        val exists = runBlocking {
            ds.connection().use { conn ->
                conn.prepareStatement("SELECT name from testing LIMIT 3;").let {
                    val ps = (it as PgPreparedStatement)
                    if (!ps.preparedStatementExists()) {
                        ps.prepare("mycursor")
                    }
                    ps.preparedStatementExists()
                }
            }
        }
        assertTrue(exists)

        ds.close()
    }

    @Test
    fun testPreparedStatement() {
        val names = runBlocking {
            ds.connection().use { conn ->

                conn.statement(
                    "insert into testing VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');"
                ).execute()

                conn.prepareStatement("SELECT name from testing LIMIT 3;")
                    .executeQuery().use { result ->
                        val names = mutableListOf<String>()
                        while (result.next()) {
                            val name = result.getString(0)
                            names.add(name)
                        }
                        names.toList()
                    }
            }
        }
        assertEquals(
            listOf(
                "foo",
                "bar",
                "baz",
            ), names
        )

        val exists = runBlocking {
            ds.connection().prepareStatement("SELECT name from testing LIMIT 3;").let {
                val ps = (it as PgPreparedStatement)
                ps.preparedStatementExists()
            }
        }
        assertTrue(exists)
    }

    @Test
    fun testInvalidPreparedStatementParams() {
        runBlocking {
            ds.connection().use { conn ->
                val statement = conn.prepareStatement("SELECT name from does_not_exist LIMIT $1;")
                    .setInt(1, 3)
                assertFails {
                    statement.executeQuery()
                }

                val count = conn.statement(
                    "insert into testing VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');"
                ).execute()
                assertEquals(3, count)
            }
        }
    }

    @Test
    fun testPreparedStatementParams() {
        val names = runBlocking {
            ds.connection().use { conn ->
                conn.statement(
                    "insert into testing VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');"
                ).execute()

                conn.prepareStatement("SELECT name from testing LIMIT $1;")
                    .setInt(1, 3)
                    .executeQuery().use { result ->
                        val names = mutableListOf<String>()
                        while (result.next()) {
                            val name = result.getString(0)
                            names.add(name)
                        }
                        names.toList()
                    }
            }
        }

        assertEquals(
            listOf(
                "foo",
                "bar",
                "baz",
            ), names
        )
    }

    @Test
    fun testUpdate() {
        runBlocking {
            ds.connection().use { conn ->
                val result  = conn.statement("insert into testing VALUES (1, 'bar'), (2, 'baz') returning *;").executeReturning()
                assertEquals(2, result.rows)

                assertTrue(result.next())
                assertEquals("bar", result.getString("name"))

                assertTrue(result.next())
//                assertEquals("bar", result.getString(1))

                assertFalse(result.next())
            }
        }
    }
}
