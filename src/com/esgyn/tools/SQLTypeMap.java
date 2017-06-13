package com.esgyn.tools;
import java.sql.Types;

/**
 * Converts database types to Java class types.
 */
public class SQLTypeMap {
    /**
     * Translates a data type from an integer (java.sql.Types value) to a string
     * that represents the corresponding class.
     * 
     * @param type
     *            The java.sql.Types value to convert to its corresponding class.
     * @return The class that corresponds to the given java.sql.Types
     *         value, or Object.class if the type has no known mapping.
     */
    /*public static Class<?> toClass(int type) {
        Class<?> result = Object.class;

        switch (type) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                result = String.class;
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                result = java.math.BigDecimal.class;
                break;

            case Types.BIT:
                result = Boolean.class;
                break;

            case Types.TINYINT:
                result = Byte.class;
                break;

            case Types.SMALLINT:
                result = Short.class;
                break;

            case Types.INTEGER:
                result = Integer.class;
                break;

            case Types.BIGINT:
                result = Long.class;
                break;

            case Types.REAL:
            case Types.FLOAT:
                result = Float.class;
                break;

            case Types.DOUBLE:
                result = Double.class;
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                result = Byte[].class;
                break;

            case Types.DATE:
                result = java.sql.Date.class;
                break;

            case Types.TIME:
                result = java.sql.Time.class;
                break;

            case Types.TIMESTAMP:
                result = java.sql.Timestamp.class;
                break;
        }

        return result;
    }*/

	public static String convert(int type) {
		String result = "";
        switch (type) {
            case Types.CHAR:
            	result = "CHAR";
            	 break;
            case Types.VARCHAR:
            	result = "VARCHAR";
            	 break;
            case Types.LONGVARCHAR:
            	result = "LONG VARCHAR";
                break;
            case Types.NUMERIC:
            	result = "NUMERIC";
                break;
            case Types.DECIMAL:
            	result = "DECIMAL";
                break;

            case Types.BIT:
            	result = "BIT";
                break;

            case Types.TINYINT:
            	result = "TINYINT";
                break;

            case Types.SMALLINT:
            	result = "SMALLINT";
                break;

            case Types.INTEGER:
            	result = "INTEGER";
                break;

            case Types.BIGINT:
            	result = "BIGINT";
                break;

            case Types.REAL:
            	result = "REAL";
                break;
            case Types.FLOAT:
            	result = "FLOAT";
                break;

            case Types.DOUBLE:
            	result = "FLOAT";
                break;

            case Types.BINARY:
            	result = "BLOB";
                break;
            case Types.VARBINARY:
            	result = "BLOB";
                break;
            case Types.LONGVARBINARY:
            	result = "LONGVARBINARY";
                break;

            case Types.DATE:
            	result = "DATE";
                break;

            case Types.TIME:
            	result = "TIME";
                break;

            case Types.TIMESTAMP:
            	result = "TIMESTAMP";
                break;
        }

        return result;
	}
}