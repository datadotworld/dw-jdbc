package world.data.jdbc.statements;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

public class DataWorldSqlParameterMetadataTest {
    private final DataWorldSqlParameterMetadata metadata;

    public DataWorldSqlParameterMetadataTest() throws SQLException {
        metadata = new DataWorldSqlParameterMetadata("select * from HallOfFame where yearid > ? order by yearid, playerID ");
    }

    @Test(expected = SQLException.class)
    public void testNullString() throws Exception {
        new DataWorldSqlParameterMetadata(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testIsWrapperFor() throws Exception {
        metadata.isWrapperFor(Class.class);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void unwrap() throws Exception {
        metadata.unwrap(Class.class);
    }

    @Test
    public void getParameterClassName() throws Exception {
        assertThat(metadata.getParameterClassName(1)).isEqualTo("org.apache.jena.graph.Node");
    }

    @Test(expected = SQLException.class)
    public void getParameterClassNameOOB() throws Exception {
        assertThat(metadata.getParameterClassName(2));
    }

    @Test
    public void getParameterCount() throws Exception {
        assertThat(metadata.getParameterCount()).isEqualTo(1);
    }

    @Test
    public void getParameterMode() throws Exception {
        assertThat(metadata.getParameterMode(1)).isEqualTo(1);
    }

    @Test(expected = SQLException.class)
    public void getParameterModeOOB() throws Exception {
        assertThat(metadata.getParameterMode(2)).isEqualTo(1);
    }

    @Test
    public void getParameterType() throws Exception {
        assertThat(metadata.getParameterType(1)).isEqualTo(Types.JAVA_OBJECT);
    }

    @Test(expected = SQLException.class)
    public void getParameterTypeOOB() throws Exception {
        assertThat(metadata.getParameterType(2));
    }

    @Test
    public void getParameterTypeName() throws Exception {
        assertThat(metadata.getParameterTypeName(1)).isEqualTo("org.apache.jena.graph.Node");
    }

    @Test(expected = SQLException.class)
    public void getParameterTypeNameOOB() throws Exception {
        assertThat(metadata.getParameterTypeName(2));
    }

    @Test
    public void getPrecision() throws Exception {
        assertThat(metadata.getPrecision(1)).isEqualTo(0);
    }

    @Test(expected = SQLException.class)
    public void getPrecisionOOB() throws Exception {
        assertThat(metadata.getPrecision(3));
    }

    @Test
    public void getScale() throws Exception {
        assertThat(metadata.getScale(1)).isEqualTo(0);
    }

    @Test(expected = SQLException.class)
    public void getScaleOOB() throws Exception {
        assertThat(metadata.getScale(-1));
    }

    @Test
    public void isNullable() throws Exception {
        assertThat(metadata.isNullable(1)).isEqualTo(0);
    }

    @Test(expected = SQLException.class)
    public void isNullableOOB() throws Exception {
        assertThat(metadata.isNullable(0));
    }

    @Test
    public void isSigned() throws Exception {
        assertThat(metadata.isSigned(1)).isFalse();
    }

    @Test(expected = SQLException.class)
    public void isSignedOOB() throws Exception {
        assertThat(metadata.isSigned(4092));

    }

}