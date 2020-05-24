package com.examples.beam.reader;

import com.examples.beam.commons.PCollectionDumpFn;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Write records to MySQL database using JDBC driver
 *
 * Options:
 * <ul>
 *   <li>host: host name</li>
 *   <li>port: port number</li>
 *   <li>user: MySQL user name</li>
 *   <li>password: MySQL login password</li>
 *   <li>database: database name to be read</li>
 *   <li>table: table name to be read</li>
 * </ul>
 */
public class JdbcReader {
  /**
   * Interface for pipeline options
   */
  public interface CustomOptions extends PipelineOptions {
    @Default.String("localhost")
    String getHost();

    @Default.Integer(3306)
    int getPort();

    @Default.String("root")
    String getUser();

    @Validation.Required
    String getPassword();

    @Validation.Required
    String getDatabase();

    @Validation.Required
    String getTable();

    void setHost(String s);

    void setPort(int i);

    void setUser(String s);

    void setDatabase(String s);

    void setTable(String s);

    void setPassword(String s);
  }

  /**
   * Get JdbcIO reader
   *
   * @param host     host name
   * @param port     port number
   * @param user     MySQL user name
   * @param password MySQL login password
   * @param database Database name
   * @param table    table name
   * @return JdbcIO reader
   */
  static JdbcIO.Read<TableRow> getReader(
      String host, int port, String user, String password,
      String database, String table) {
    String url = String.format("jdbc:mysql://%s:%d/%s",
        host, port, database);

    // JDBC connection configuration
    JdbcIO.DataSourceConfiguration config = JdbcIO
        .DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver", url)
        .withUsername(user)
        .withPassword(password);

    // Query
    String query = String.format("SELECT * FROM `%s`.`%s`;", database, table);

    return JdbcIO.<TableRow>read()
        .withDataSourceConfiguration(config)
        .withQuery(query)
        .withRowMapper(new CustomRowMapper())
        .withCoder(TableRowJsonCoder.of());
  }

  /**
   * Define pipeline graph and execute
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CustomOptions opt = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(CustomOptions.class);
    Pipeline pipeline = Pipeline.create(opt);

    pipeline
        .apply(getReader(
            opt.getHost(), opt.getPort(), opt.getUser(), opt.getPassword(),
            opt.getDatabase(), opt.getTable()))
        .apply(ParDo.of(new PCollectionDumpFn<>()));

    pipeline.run();
  }

  /**
   * Map records into BigQuery table row
   */
  private static class CustomRowMapper implements JdbcIO.RowMapper<TableRow> {
    @Override
    public TableRow mapRow(ResultSet resultSet) throws SQLException {
      // output table row
      TableRow row = new TableRow();

      // loop over columns
      ResultSetMetaData meta = resultSet.getMetaData();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        String name = meta.getColumnName(i);
        JDBCType type = JDBCType.valueOf(meta.getColumnType(i));
        switch (type) {
          // cast into long
          case BIT:
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
            row.set(name, resultSet.getLong(i));
            break;
          // cast into double
          case REAL:
          case FLOAT:
          case DOUBLE:
            row.set(name, resultSet.getDouble(i));
            break;
          // cast into BigDecimal
          case DECIMAL:
            row.set(name, resultSet.getBigDecimal(i));
            break;
          // cast into String
          case CHAR:
          case VARCHAR:
          case LONGVARCHAR:
            row.set(name, resultSet.getString(i));
            break;
          // cast into com.google.cloud.ByteArray
          case BINARY:
          case VARBINARY:
          case LONGVARBINARY:
            row.set(name, ByteArray.copyFrom(resultSet.getBytes(i)));
            break;
          // cast into java.sql.Time
          case TIME:
            // value should be in range 00:00:00 ~ 23:59:59
            row.set(name, resultSet.getTime(i));
            break;
          // cast into com.google.cloud.Date
          case DATE:
            row.set(name, Date.fromJavaUtilDate(resultSet.getDate(i)));
            break;
          // cast into com.google.cloud.Timestamp
          case TIMESTAMP:
            row.set(name, Timestamp.of(resultSet.getTimestamp(i)));
            break;
          // Error
          default:
            throw new RuntimeException("Unsupported column type: " + type);
        }
      }
      return row;
    }
  }
}
