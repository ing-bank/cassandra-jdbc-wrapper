
package com.ing.data.cassandra.jdbc.metadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static schemacrawler.test.utility.ExecutableTestUtility.executableExecution;
import static schemacrawler.test.utility.FileHasContent.classpathResource;
import static schemacrawler.test.utility.FileHasContent.hasSameContentAs;
import static schemacrawler.test.utility.FileHasContent.outputOf;
import java.net.InetSocketAddress;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import schemacrawler.schemacrawler.InfoLevel;
import schemacrawler.schemacrawler.LimitOptionsBuilder;
import schemacrawler.schemacrawler.LoadOptionsBuilder;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.command.text.schema.options.SchemaTextOptions;
import schemacrawler.tools.command.text.schema.options.SchemaTextOptionsBuilder;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSources;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

/****
	* Runs extensive database metadata tests.
****/
@Testcontainers
public class CompleteMetadataTest {

    private final DockerImageName imageName = DockerImageName.parse("cassandra");

    @Container
    private final CassandraContainer<?> dbContainer =
            new CassandraContainer<>(imageName.withTag("5.0")).withInitScript("initEmbeddedDse.cql");

    private DatabaseConnectionSource dbConnectionSource;

    /****
        * Sets up a data source to connect to the Cassandra database.
        *
    ****/
    @BeforeEach
    public void createDatabase() {

        if (!dbContainer.isRunning()) {
            fail("Testcontainer for database is not available");
        }

        final InetSocketAddress contactPoint = dbContainer.getContactPoint();
        final String host = contactPoint.getHostName();
        final int port = contactPoint.getPort();
        final String keyspace = "test_keyspace_vect";
        final String localDatacenter = dbContainer.getLocalDatacenter();
        final String connectionUrl = String.format("jdbc:cassandra://%s:%d/%s?localdatacenter=%s", host,
                port, keyspace, localDatacenter);
        System.out.printf("url=%s%n", connectionUrl);
        createDataSource(connectionUrl);
    }

    /****
        * Uses SchemaCrawler to obtain database metadata in "maximum" mode. 
        * Compares actual results to an expected file with results. The test will
        * produce a new expected results file if the actual and expected results
        * do not match.
		*
		* @throws Exception on an error when running the test
    ****/
    @Test
    public void givenDatabase_whenCompleteMetadataExtracted_shouldMatchExpectedOutput()
            throws Exception {

        final LimitOptionsBuilder limitOptionsBuilder =
                LimitOptionsBuilder.builder().includeSchemas(Pattern.compile("test_keyspace_vect"));
        final SchemaInfoLevelBuilder schemaInfoLevelBuilder =
                SchemaInfoLevelBuilder.builder().withInfoLevel(InfoLevel.maximum);
        final LoadOptionsBuilder loadOptionsBuilder =
                LoadOptionsBuilder.builder().withSchemaInfoLevelBuilder(schemaInfoLevelBuilder);
        final SchemaCrawlerOptions schemaCrawlerOptions = SchemaCrawlerOptionsBuilder
                .newSchemaCrawlerOptions().withLimitOptions(limitOptionsBuilder.toOptions())
                .withLoadOptions(loadOptionsBuilder.toOptions());
        final SchemaTextOptionsBuilder textOptionsBuilder = SchemaTextOptionsBuilder.builder();
        textOptionsBuilder.showDatabaseInfo().showJdbcDriverInfo();
        final SchemaTextOptions textOptions = textOptionsBuilder.toOptions();

        final SchemaCrawlerExecutable executable = new SchemaCrawlerExecutable("details");
        executable.setSchemaCrawlerOptions(schemaCrawlerOptions);
        executable.setAdditionalConfiguration(SchemaTextOptionsBuilder.builder(textOptions).toConfig());

        final String expectedResource = "expected_metadata_output.txt";
        assertThat(outputOf(executableExecution(getDataSource(), executable)),
                hasSameContentAs(classpathResource(expectedResource)));
    }

    private void createDataSource(final String connectionUrl) {
        dbConnectionSource = DatabaseConnectionSources.newDatabaseConnectionSource(connectionUrl,
                new MultiUseUserCredentials(dbContainer.getUsername(), dbContainer.getPassword()));

    }

    private DatabaseConnectionSource getDataSource() {
        return dbConnectionSource;
    }
}
