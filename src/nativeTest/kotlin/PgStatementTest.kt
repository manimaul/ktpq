import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


const val testDbSql = """
    create table if not exists testing 
    (
        id      BIGSERIAL PRIMARY KEY, 
        name    VARCHAR   UNIQUE NOT NULL,
        json_b  JSONB     NULL,
        array_t VARCHAR[] NULL
    );
"""

class PgStatementTest {

    lateinit var ds: PgDataSource

    @BeforeTest
    fun beforeEach() {
        ds = PgDataSource("postgresql://admin:mysecretpassword@localhost:5432/s57server", 1)
        assertEquals(1, ds.readyCount)
        runBlocking {
            ds.connection().use { conn ->
                conn.statement("drop table if exists testing;").execute()
                conn.statement(testDbSql).execute()
            }
        }
    }

    @AfterTest
    fun afterEach() {
        ds.close()
    }

    @Test
    fun testStatement() {
        val chartNames = runBlocking {
            ds.connection().use { conn ->
                conn.statement(
                    "insert into testing VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');"
                ).execute()
                conn.statement("SELECT name from testing LIMIT 3;").executeQuery().use { result ->
                    val names = mutableListOf<String>()
                    while (result.next()) {
                        val name = result.getString(0)
                        names.add(name)
                    }
                    names
                }
            }
        }

        assertEquals(
            listOf(
                "foo",
                "bar",
                "baz",
            ), chartNames
        )
    }

    @Test
    fun testStatementParams() {
        val names = runBlocking {
            ds.connection().use { conn ->
                conn.statement(
                    "insert into testing VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');"
                ).execute()
                conn.statement("SELECT name from testing LIMIT $1;").setInt(1, 2).executeQuery().use { result ->
                    val names = mutableListOf<String>()
                    println("result rows ${result.rows}")
                    while (result.next()) {
                        val name = result.getString(0)
                        names.add(name)
                    }
                    names
                }
            }
        }

        assertEquals(
            listOf(
                "foo",
                "bar",
            ), names
        )
    }

    @OptIn(ExperimentalForeignApi::class)
    @Test
    fun testExecute() {
        runBlocking {
            ds.connection().use { conn ->
                val count = conn.statement("insert into testing VALUES (1, 'bar'), (2, 'baz');").execute()
                val stmt = conn.statement("insert into testing VALUES ($1, $2), ($3, $4);")
                    .setLong(1, 3L)
                    .setString(2, "rab")
                    .setLong(3, 4L)
                    .setString(4, "zab")
                assertEquals(4, (stmt as PgStatement).parameters)
                val count2 = stmt.execute()
                assertEquals(2, count)
                assertEquals(2, count2)
            }

            ds.connection().use { conn ->
                conn.statement("select * from testing;").executeQuery().use { results ->
                    while (results.next()) {
                        println("${results.getLong("id")},${results.getString("name")}")
                    }
                }
            }
        }
    }

    @Test
    fun testExecute500Params() {
        runBlocking {
            ds.connection().use { conn ->
                var sql = "insert into testing VALUES "
                (1..499).forEach {
                    if (it % 2 == 1) {
                        sql += "(\$$it, \$${it + 1})${if (it == 499) ";" else ", "}"
                    }
                }
                println("creating statement for sql = '$sql'")
                val stmt = conn.statement(sql)
                assertEquals(500, (stmt as PgStatement).parameters)
                println("binding params")
                (1..499).forEach {
                    if (it % 2 == 1) {
                        stmt.setLong(it, 10000L + it)
                        stmt.setString(it + 1, "value ${it + 1}")
                    }
                }
                println("executing insert")
                stmt.execute()

                var count = 0
                println("creating select statement")
                val select = conn.statement("select * from testing;")
                println("executing select staement")
                select.executeQuery().use { resultSet ->
                    assertEquals(250, resultSet.rows)
                    while (resultSet.next()) {
                        assertEquals(250, resultSet.rows)
                        val id = resultSet.getLong(0)
                        val name = resultSet.getString(1)
                        println("result record id: $id value = $name")
                        count++
                    }
                }
                assertEquals(250, count)
            }

            ds.connection().use { conn ->
                var count = 0
                conn.prepareStatement("select * from testing;").executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val id = resultSet.getLong("id")
                        val name = resultSet.getString("name")
                        println("result record id: $id value = $name")
                        count++
                    }
                }
                assertEquals(250, count)
                count = 0
                conn.statement("select * from testing;").executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val id = resultSet.getLong(0)
                        val name = resultSet.getString(1)
                        println("result record id: $id value = $name")
                        count++
                    }
                }
                assertEquals(250, count)
            }
        }
    }

    @Test
    fun testUpdates() {
        runBlocking {
            ds.connection().use { conn ->
                println("===== inserting bar, baz")
                val bazId =
                    conn.statement("insert into testing VALUES (1, 'bar'), (2, 'baz') returning *;").executeReturning()
                        .use { result ->
                            assertEquals(2, result.rows)

                            assertTrue(result.next())
                            println("${result.getLong("id")}, ${result.getString("name")}")
                            assertEquals("bar", result.getString("name"))

                            assertTrue(result.next())
                            println("${result.getLong("id")}, ${result.getString("name")}")
                            val id = result.getLong("id")
                            assertEquals("baz", result.getString("name"))

                            assertFalse(result.next())
                            id
                        }

                println("===== updating bazId $bazId name to foo")
                conn.statement("update testing set name=$1 where id=$2 returning *;")
                    .setString(1, "foo")
                    .setLong(2, bazId)
                    .executeReturning().use { result ->
                        assertEquals(1, result.rows)
                        assertTrue(result.next())
                        println("${result.getLong("id")}, ${result.getString("name")}")
                        assertEquals("foo", result.getString("name"))
                    }

                println("===== updating bazId $bazId name to pil")
                conn.statement("update testing set name=$1 where id=$2 returning *;")
                    .setString(1, "pil")
                    .setLong(2, bazId)
                    .executeReturning().let { result ->
                        assertEquals(1, result.rows)
                        assertTrue(result.next())
                        println("${result.getLong("id")}, ${result.getString("name")}")
                        assertEquals("pil", result.getString("name"))
                    }

                println("===== querying bazId $bazId expecting name to be pil")
                conn.statement("select * from testing;").executeQuery().use { result ->
                    val list = mutableListOf<String>()
                    while (result.next()) {
                        list.add("${result.getLong("id")},${result.getString("name")}")
                    }
                    list.forEach {
                        println(it)
                    }
                    assertTrue(list.contains("$bazId,pil"))
                    assertEquals(2, list.size)
                }
//                conn.statement("select id, name from testing where name=$1 ;")
//                    .setString(1, "pil").executeQuery().use { result ->
//                        assertEquals(1, result.rows)
//                        assertTrue(result.next())
//                        println("${result.getLong("id")}, ${result.getString("name")}")
//                        assertEquals("pil", result.getString("name"))
//                    }
            }
        }
    }

    @Test
    fun testBinaryData() {
        runBlocking {
            ds.connection().use { conn ->
//                val count = conn.statement("insert into testing VALUES (1, 'bar'), (2, 'baz');").execute()
//                val stmt = conn.statement("insert into testing VALUES ($1, $2), ($3, $4);")
//                assertEquals(4, (stmt as PgStatement).parameters)
//
//                stmt.setLong(1, 3L).setString(2, "rab").setLong(3, 4L).setString(4, "zab")
//
//                val countParms = stmt.execute()
//
//                assertEquals(2, count)
//                assertEquals(2, countParms)
            }
        }
    }
}
