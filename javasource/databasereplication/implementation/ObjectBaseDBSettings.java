package databasereplication.implementation;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.MendixRuntimeException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databasereplication.implementation.DbTypes.DbConnectionFactory;
import databasereplication.implementation.DbTypes.IDatabaseConnector;
import databasereplication.interfaces.IDatabaseSettings;
import databasereplication.proxies.DBType;
import databasereplication.proxies.Database;
import databasereplication.proxies.TableFilter;

public class ObjectBaseDBSettings extends IDatabaseSettings {
	private static final String CIPHER_NAME = "AES/GCM/NoPadding";
	private static final String LEGACY_CIPHER_NAME = "AES/CBC/PKCS5PADDING";

	private IMendixObject dbObject;
	private IContext context;
	private FilterList tablesFilter;

	public ObjectBaseDBSettings( IContext context, IMendixObject databaseObject ) {
		this.dbObject = databaseObject;
		this.context = context;
	}

	@Override
	public String getAddress() {
		String dbAddress = this.dbObject.getValue(this.getContext(), Database.MemberNames.DatabaseURL.toString());
		return trim(dbAddress);
	}

	@Override
	public String getDatabaseName() {
		String dbName = this.dbObject.getValue(this.getContext(), Database.MemberNames.DatabaseName.toString());
		return trim(dbName);
	}


	@Override
	public IDatabaseConnector getDatabaseConnection() {
		DbConnectionFactory dbCFactory = DbConnectionFactory.getInstance(
				DBType.valueOf((String) this.dbObject.getValue(this.getContext(), Database.MemberNames.DatabaseType.toString())), this);
		return dbCFactory.getConnector();
	}

	@Override
	public String getPassword() {
		String dbPassword = this.dbObject.getValue(this.getContext(), Database.MemberNames.DatabasePassword_Encrypted.toString());
		if ( dbPassword != null && !"".equals(dbPassword) )
			return decryptString(dbPassword);

		return this.dbObject.getValue(this.getContext(), Database.MemberNames.DatabasePassword.toString());
	}

	@Override
	public String getPort() {
		Integer value = this.dbObject.getValue(this.getContext(), Database.MemberNames.Port.toString());
		if ( value == null ) {
			return "";
		}
	
		return String.valueOf(value);
	}

	@Override
	public String getServiceName() {
		String service = this.dbObject.getValue(this.getContext(), Database.MemberNames.ServiceName.toString());
		return trim(service);
	}

	@Override
	public String getUserName() {
		String user = this.dbObject.getValue(this.getContext(), Database.MemberNames.DatabaseUser.toString());
		return trim(user);
	}

	@Override
	public IContext getContext() {
		return this.context;
	}

	@Override
	public boolean useIntegratedAuthentication() {
		return (Boolean) this.dbObject.getValue(this.getContext(), Database.MemberNames.useIntegratedSecurity.toString());
	}

	public static String decryptString(String valueToDecrypt) {
		if (valueToDecrypt == null)
			return null;

		try {
			final String[] s = valueToDecrypt.split(";");
			final Decoder decoder = Base64.getDecoder();
			final byte[] decodedVector = decoder.decode(s[s.length - 2]);
			final SecretKeySpec secretKey = createSecretKey();
			final Cipher cipher;
			switch (s.length) {
			case 2:
				cipher = Cipher.getInstance(LEGACY_CIPHER_NAME);
				cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(decodedVector));
				break;
			case 3:
				cipher = Cipher.getInstance(s[0]);
				cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(secretKey.getEncoded().length * 8, decodedVector));
				break;
			default:
				return valueToDecrypt; // Not an encrypted string, just return the original value.
			}

			final byte[] decodedEncryptedData = decoder.decode(s[s.length - 1]);
			final byte[] originalData = cipher.doFinal(decodedEncryptedData);
			return new String(originalData, StandardCharsets.UTF_8);
		} catch (Exception e) {
			throw new MendixRuntimeException("Unable to decrypt the password", e);
		}
	}

	public static String encryptString(String valueToEncrypt) throws Exception {
		if (valueToEncrypt == null)
			return null;

		final Cipher cipher = Cipher.getInstance(CIPHER_NAME);
		cipher.init(Cipher.ENCRYPT_MODE, createSecretKey());

		final byte[] encryptedData = cipher.doFinal(valueToEncrypt.getBytes(StandardCharsets.UTF_8));

		final Encoder encoder = Base64.getEncoder();
		final String encodedVector = encoder.encodeToString(cipher.getIV());
		final String encodedEncryptedData = encoder.encodeToString(encryptedData);
		return CIPHER_NAME + ';' + encodedVector + ';' + encodedEncryptedData;
	}

	private static SecretKeySpec createSecretKey() {
		final String secretKey = Core.getConfiguration().getConstantValue("DatabaseReplication.SecretKey").toString();
		return new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "AES");
	}

	@Override
	public FilterList getTableFilters() throws CoreException {
		this.tablesFilter = new FilterList();
		List<IMendixObject> result = Core.createXPathQuery(String.format("//%s[%s=$value]",
				TableFilter.entityName,
				TableFilter.MemberNames.TableFilter_Database.toString()))
				.setVariable("value",  this.dbObject.getId())
				.execute(getContext());
		if ( result.size() > 0 ) {
			for( IMendixObject resultObj : result ) {
				this.tablesFilter.add((String) resultObj.getValue(getContext(), TableFilter.MemberNames.Filter.toString()));
			}
		}

		return this.tablesFilter;
	}

	@Override
	public IMendixIdentifier getCustomConnectionInfo() {
		return this.dbObject.getValue(this.context, Database.MemberNames.Database_CustomConnectionInfo.toString());
	}
	
	private String trim(String s) {
		return (s == null) ? "" : s.trim();
	}
}
