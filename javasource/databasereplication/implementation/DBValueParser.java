package databasereplication.implementation;

import com.mendix.replication.AbstractValueExtractor;
import com.mendix.replication.ICustomValueParser;
import com.mendix.replication.ParseException;
import com.mendix.replication.ReplicationSettings;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DBValueParser extends AbstractValueExtractor {

	public DBValueParser( ReplicationSettings settings ) {
		super(settings);
	}

	@Override
	public Object getValue( PrimitiveType type, String columnAlias, Object value ) throws ParseException {
		if ( value instanceof ResultSet ) {
			try {
				return super.getValue(type, columnAlias, ((ResultSet) value).getObject(columnAlias));
			}
			catch( SQLException e ) {
				throw new ParseException("Could not get the value for column: " + columnAlias, e);
			}
		}
		
		return super.getValue(type, columnAlias, value);
	}

	public Object getValueFromDataSet( String keyAlias, PrimitiveType type, Object dataSet ) throws ParseException {
		try {
			return ((ResultSet) dataSet).getObject(keyAlias);
		}
		catch( SQLException e ) {
			throw new ParseException("Unable to find field: " + keyAlias + " in the resultSet", e);
		}
	}

	@Override
	public String getKeyValueFromAlias(Object recordDataSet, String keyAlias) throws ParseException {
		String keyValue;
		
		if ( this.customValueParsers.containsKey(keyAlias) ) {
			ICustomValueParser vp = this.customValueParsers.get(keyAlias);
			Object value = vp.parseValue(getValueFromDataSet(keyAlias, PrimitiveType.String, recordDataSet));

			keyValue = getTrimmedValue(value, keyAlias);
		}
		else
			keyValue = getKeyValueByPrimitiveType(this.getSettings().getMemberType(keyAlias), keyAlias,
					getValueFromDataSet(keyAlias, this.getSettings().getMemberType(keyAlias), recordDataSet));
		
		return keyValue;
	}
}
