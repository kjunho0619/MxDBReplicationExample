// This file was generated by Mendix Studio Pro.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package databasereplication.actions;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.core.objectmanagement.member.MendixObjectReferenceSet;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.webui.CustomJavaAction;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import databasereplication.implementation.DBUpdateSettings;
import databasereplication.implementation.ObjectBaseDBSettings;
import databasereplication.proxies.Database;

/**
 * Using this Java action you can push multiple objects to the external database all within the same database connection.
 * 
 * The functionality is identical to the UpdateByMapping microflow, except the action will setup a single connection and try to merge the insert & update queries if possible. 
 */
public class UpdateByMapping_UsingUpdateSet extends CustomJavaAction<java.lang.Boolean>
{
	/** @deprecated use Mapping.getMendixObject() instead. */
	@java.lang.Deprecated(forRemoval = true)
	private final IMendixObject __Mapping;
	private final databasereplication.proxies.TableMapping Mapping;
	/** @deprecated use UpdateSetObject.getMendixObject() instead. */
	@java.lang.Deprecated(forRemoval = true)
	private final IMendixObject __UpdateSetObject;
	private final databasereplication.proxies.UpdateSet UpdateSetObject;
	/** @deprecated use UpdateConfig.getMendixObject() instead. */
	@java.lang.Deprecated(forRemoval = true)
	private final IMendixObject __UpdateConfig;
	private final databasereplication.proxies.UpdateConfiguration UpdateConfig;

	public UpdateByMapping_UsingUpdateSet(
		IContext context,
		IMendixObject _mapping,
		IMendixObject _updateSetObject,
		IMendixObject _updateConfig
	)
	{
		super(context);
		this.__Mapping = _mapping;
		this.Mapping = _mapping == null ? null : databasereplication.proxies.TableMapping.initialize(getContext(), _mapping);
		this.__UpdateSetObject = _updateSetObject;
		this.UpdateSetObject = _updateSetObject == null ? null : databasereplication.proxies.UpdateSet.initialize(getContext(), _updateSetObject);
		this.__UpdateConfig = _updateConfig;
		this.UpdateConfig = _updateConfig == null ? null : databasereplication.proxies.UpdateConfiguration.initialize(getContext(), _updateConfig);
	}

	@java.lang.Override
	public java.lang.Boolean executeAction() throws Exception
	{
		// BEGIN USER CODE
		if( this.Mapping == null )
			throw new CoreException("Please provide a valid mapping");
		if( this.Mapping == null )
			throw new CoreException("Please provide an object to send to the other database");
		
		IContext context = this.getContext();
		Database CurDatabase = this.Mapping.getTableMapping_Database(context);
		ObjectBaseDBSettings dbSettings = new ObjectBaseDBSettings( context, CurDatabase.getMendixObject() );
		
		
		DBUpdateSettings settings = UpdateByMapping.setupUpdateSettings(context,this.Mapping, this.UpdateConfig, dbSettings);
		
		for( MendixObjectReferenceSet refSet : this.__UpdateSetObject.getReferenceSets(context) ) {
			
			for( IMendixIdentifier id : refSet.getValue(context) ) {
				IMendixObject obj = Core.retrieveId(context, id);
				UpdateByMapping.doDatabaseUpdate(dbSettings, obj, settings);
			}
		}
		

		return true;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 * @return a string representation of this action
	 */
	@java.lang.Override
	public java.lang.String toString()
	{
		return "UpdateByMapping_UsingUpdateSet";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
