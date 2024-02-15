// Copyright Nikita Zolotukhin. All Rights Reserved.

#include "CookedAssetWriter.h"
#include "IoStorePackageMap.h"
#include "ZenTools.h"
#include "Dom/JsonObject.h"
#include "HAL/FileManager.h"
#include "Internationalization/Regex.h"
#include "Misc/Paths.h"
#include "Serialization/MemoryWriter.h"
#include "UObject/Class.h"
#include "UObject/Package.h"
#include "UObject/SoftObjectPath.h"
#include "Misc/FileHelper.h"
#include "Serialization/JsonSerializer.h"

FAssetSerializationWriter::FAssetSerializationWriter( FArchive& Ar, FAssetSerializationContext* Context ) : FArchiveProxy( Ar ), Context( Context )
{
}

FArchive& FAssetSerializationWriter::operator<<(FName& Value)
{
	const FName NameWithoutNumber = FName( Value, NAME_NO_NUMBER_INTERNAL );

	if ( Context->bSerializingNameMap )
	{
		checkf( Context->NameReverseLookupMap.Contains( NameWithoutNumber ), TEXT("Attempt to serialize Name '%s' that is not in the NameMap"), *Value.ToString() );

		NameWithoutNumber.GetDisplayNameEntry()->Write( *this );
	}
	else
	{
		int32 NameIndex;
		int32 NameNumber = Value.GetNumber();
	
		if ( const int32* ExistingNameIndex = Context->NameReverseLookupMap.Find( NameWithoutNumber ) )
		{
			NameIndex = *ExistingNameIndex;
		}
		else
		{
			checkf( !Context->bNameMapWrittenToFile, TEXT("New Name '%s' serialized after NameMap has already been written to the disk"), *Value.ToString() );

			NameIndex = Context->NameMap.Add( NameWithoutNumber );
			Context->NameReverseLookupMap.Add( NameWithoutNumber, NameIndex );
		}

		*this << NameIndex;
		*this << NameNumber;
	}
	
	return *this;
}

void FAssetSerializationWriter::SetFilterEditorOnly( bool InFilterEditorOnly )
{
	FArchiveProxy::SetFilterEditorOnly( InFilterEditorOnly );
	FArchive::SetFilterEditorOnly( InFilterEditorOnly );
}

FCookedAssetWriter::FCookedAssetWriter(const TSharedPtr<FIoStorePackageMap>& InPackageMap, const FString& InOutputDir) : PackageMap( InPackageMap ), RootOutputDir( InOutputDir ), NumPackagesWritten( 0 )
{
}

void FCookedAssetWriter::WritePackagesFromContainer( const TSharedPtr<FIoStoreReader>& Reader, const FString& PackageFilter )
{
	const FIoContainerId ContainerId = Reader->GetContainerId();
	UE_LOG( LogIoStoreTools, Display, TEXT("Writing asset files for Container %lld"), ContainerId.Value() );

	FPackageContainerMetadata ContainerMetadata;
	if ( PackageMap->FindPackageContainerMetadata( ContainerId, ContainerMetadata ) )
	{
		const TFunction<bool(const FPackageId&)> PackageFilterFunction = MakePackageFilterFunction( PackageFilter );
		
		for ( const FPackageId& PackageId : ContainerMetadata.PackagesInContainer )
		{
			if ( PackageFilterFunction( PackageId ) )
			{
				WriteSinglePackage( PackageId, false, Reader );
			}
		}
		for ( const FPackageId& OptionalPackageId : ContainerMetadata.OptionalPackagesInContainer )
		{
			if ( PackageFilterFunction( OptionalPackageId ) )
			{
				WriteSinglePackage( OptionalPackageId, true, Reader );
			}
		}
	}
}

TFunction<bool(const FPackageId&)> FCookedAssetWriter::MakePackageFilterFunction(const FString& PackageFilter) const
{
	// No filter is specified, we write all packages
	if ( PackageFilter.IsEmpty() )
	{
		return [](const FPackageId&) { return true; };
	}

	// Regex match filter
	if ( PackageFilter.StartsWith("!") )
	{
		const FRegexPattern RegexPattern( PackageFilter.RightChop( 1 ) );
		
		return [this, RegexPattern](const FPackageId& PackageId )
		{
			const FName PackageName = PackageMap->FindPackageName( PackageId );
			FRegexMatcher RegexMatcher( RegexPattern, PackageName.ToString() );

			return RegexMatcher.FindNext();
		};
	}

	// Normal prefix-based match filter
	return [this, PackageFilter](const FPackageId& PackageId )
	{
		const FName PackageName = PackageMap->FindPackageName( PackageId );

		TStringBuilder<256> PackageNameBuffer;
		PackageName.ToString( PackageNameBuffer );

		return TStringView<TCHAR>(PackageNameBuffer).StartsWith( PackageFilter );
	};
}

void FCookedAssetWriter::WritePackageStoreManifest() const
{
	const FString PackageStoreFilename = RootOutputDir / TEXT("PackageStoreManifest.json");
	IFileManager::Get().MakeDirectory( *FPaths::GetPath( PackageStoreFilename ), true );

	const TSharedPtr<FJsonObject> RootObject = MakeShared<FJsonObject>();

	TStringBuilder<64> ChunkIdStringBuilder;
	auto ChunkIdToString = [&ChunkIdStringBuilder](const FIoChunkId& ChunkId)
	{
		ChunkIdStringBuilder.Reset();
		ChunkIdStringBuilder << ChunkId;
		return *ChunkIdStringBuilder;
	};
	
	TArray<TSharedPtr<FJsonValue>> FilesArray;
	for ( const TPair<FIoChunkId, FString>& FilePair : ChunkIdToSavedFileMap )
	{
		const TSharedPtr<FJsonObject> FileObject = MakeShared<FJsonObject>();
		FileObject->SetStringField( TEXT("Path"), FilePair.Value );
		FileObject->SetStringField( TEXT("ChunkId"), ChunkIdToString( FilePair.Key ) );

		FilesArray.Add( MakeShared<FJsonValueObject>( FileObject ) );
	}
	RootObject->SetArrayField( TEXT("Files"), FilesArray );

	TArray<TSharedPtr<FJsonValue>> PackagesArray;
	for ( const TPair<FName, FSavedPackageInfo>& SavedPackageInfo : SavedPackageMap )
	{
		const TSharedPtr<FJsonObject> PackageObject = MakeShared<FJsonObject>();
		PackageObject->SetStringField( TEXT("Name"), SavedPackageInfo.Key.ToString() );

		if ( !SavedPackageInfo.Value.ExportBundleChunks.Num() == 0 )
		{
			TArray<TSharedPtr<FJsonValue>> ExportBundleChunkIdsArray;
			for ( const FIoChunkId& ExportBundleChunkId : SavedPackageInfo.Value.ExportBundleChunks )
			{
				ExportBundleChunkIdsArray.Add( MakeShared<FJsonValueString>( ChunkIdToString( ExportBundleChunkId ) ) );
			}
			PackageObject->SetArrayField( TEXT("ExportBundleChunkIds"), ExportBundleChunkIdsArray );
		}

		if ( !SavedPackageInfo.Value.BulkDataChunks.Num() == 0 )
		{
			TArray<TSharedPtr<FJsonValue>> BulkDataChunkIdsArray;
			for ( const FIoChunkId& BulkDataChunkId : SavedPackageInfo.Value.BulkDataChunks )
			{
				BulkDataChunkIdsArray.Add( MakeShared<FJsonValueString>( ChunkIdToString( BulkDataChunkId ) ) );
			}
			PackageObject->SetArrayField( TEXT("BulkDataChunkIds"), BulkDataChunkIdsArray );
		}
		PackagesArray.Add( MakeShared<FJsonValueObject>( PackageObject ) );
	}
	RootObject->SetArrayField( TEXT("Packages"), PackagesArray );

	FString ResultJsonString;
	FJsonSerializer::Serialize( RootObject.ToSharedRef(), TJsonWriterFactory<>::Create( &ResultJsonString ) );

	check( FFileHelper::SaveStringToFile( ResultJsonString, *PackageStoreFilename ) );
	UE_LOG( LogIoStoreTools, Display, TEXT("Written PackageStore Manifest to '%s'"), *PackageStoreFilename );
}

void FCookedAssetWriter::WriteSinglePackage( FPackageId PackageId, bool bIsOptionalSegmentPackage, const TSharedPtr<FIoStoreReader>& Reader )
{
	FPackageMapExportBundleEntry ExportBundleEntry;
	checkf( PackageMap->FindExportBundleData( PackageId, ExportBundleEntry ), TEXT("Failed to find export bundle entry for PackageId %lld"), PackageId.ValueForDebugging() );
	
	const FString PackageFilename = RootOutputDir / ExportBundleEntry.PackageName.ToString();
	IFileManager::Get().MakeDirectory( *FPaths::GetPath( PackageFilename ), true );

	UE_LOG( LogIoStoreTools, Display, TEXT("Beginning writing package '%s' (0x%llx) to file '%s'"), *ExportBundleEntry.PackageName.ToString(), PackageId.Value(), *PackageFilename );

	// Initialize serialization context
	FAssetSerializationContext SerializationContext{};
	
	SerializationContext.PackageId = PackageId;
	SerializationContext.PackageHeaderFilename = PackageFilename;
	SerializationContext.BundleData = &ExportBundleEntry;
	SerializationContext.IoStoreReader = Reader.Get();

	FSavedPackageInfo& SavedPackageInfo = SavedPackageMap.FindOrAdd( SerializationContext.BundleData->PackageName );
	SavedPackageInfo.ExportBundleChunks.Add( SerializationContext.BundleData->PackageChunkId );

	// Populate package summary, and also process imports and exports
	ProcessPackageSummaryAndNamesAndExportsAndImports( SerializationContext );

	// Serialize exports into the separate file (event driven loader expects that)
	{
		FString ExtensionString = ".uexp";
		
		// Optional segment packages have .o prefix before their extensions, e.g.
		if ( bIsOptionalSegmentPackage )
		{
			ExtensionString.InsertAt( 0, TEXT(".o") );
		}
		const FString ExportsFilename = FPaths::SetExtension( SerializationContext.PackageHeaderFilename, ExtensionString );

		const TUniquePtr<FArchive> ExportsArchive( IFileManager::Get().CreateFileWriter( *ExportsFilename, FILEWRITE_EvenIfReadOnly ) );
		checkf( ExportsArchive.IsValid(), TEXT("Failed to load exports file '%s'"), *ExportsFilename );
	
		// Write the exports. This will also fix-up serial offsets on the export map entries in the summary
		WritePackageExports( *ExportsArchive, SerializationContext );
		ExportsArchive->Flush();
	}

	// Serialize package summary and other necessary data into the main asset header file
	{
		FString ExtensionString = ( SerializationContext.Summary.PackageFlags & PKG_ContainsMap ) != 0 ? ".umap" : ".uasset";

		// Optional segment packages have .o prefix before their extensions, e.g.
		if ( bIsOptionalSegmentPackage )
		{
			ExtensionString.InsertAt( 0, TEXT(".o") );
		}
		const FString HeaderFilename = FPaths::SetExtension( SerializationContext.PackageHeaderFilename, ExtensionString );
		
		FString RelativeFilename = FPaths::SetExtension( PackageFilename, ExtensionString );
		ChunkIdToSavedFileMap.Add( SerializationContext.BundleData->PackageChunkId, RelativeFilename );

		const TUniquePtr<FArchive> HeaderArchive( IFileManager::Get().CreateFileWriter( *HeaderFilename, FILEWRITE_EvenIfReadOnly ) );
		checkf( HeaderArchive.IsValid(), TEXT("Failed to open header file '%s'"), *HeaderFilename );

		FAssetSerializationWriter ProxyWriter( *HeaderArchive, &SerializationContext );
		WritePackageHeader( ProxyWriter, SerializationContext );
		HeaderArchive->Flush();
	}

	// Write bulk data
	WriteBulkData( SerializationContext );

	// Notify the user that we have finished writing the asset
	UE_LOG( LogIoStoreTools, Display, TEXT("Serialized Package '%s' to '%s'"), *SerializationContext.BundleData->PackageName.ToString(), *SerializationContext.PackageHeaderFilename );
	NumPackagesWritten++;
}

FPackageIndex FCookedAssetWriter::FindExistingObjectImport( FPackageIndex OuterIndex, FName ObjectName, FAssetSerializationContext& Context )
{
	for ( int32 ImportIndex = 0; ImportIndex < Context.ImportMap.Num(); ImportIndex++ )
	{
		const FObjectImport& ExistingObjectImport = Context.ImportMap[ ImportIndex ];

		if ( ExistingObjectImport.OuterIndex == OuterIndex && ExistingObjectImport.ObjectName == ObjectName )
		{
			return FPackageIndex::FromImport( ImportIndex );
		}
	}
	return FPackageIndex();
}

FPackageIndex FCookedAssetWriter::CreatePackageImport( FName PackageName, FAssetSerializationContext& Context )
{
	// Package import of our own package is always 0
	if ( PackageName == Context.BundleData->PackageName )
	{
		return FPackageIndex();
	}
	FPackageIndex ImportedPackageIndex = FindExistingObjectImport( FPackageIndex(), PackageName, Context );
	
	if ( ImportedPackageIndex.IsNull() )
	{
		const int32 ImportIndex = Context.ImportMap.AddDefaulted();
		FObjectImport& NewPackageImport = Context.ImportMap[ ImportIndex ];

		const FSoftObjectPath ClassPath = UPackage::StaticClass()->GetPathName();

		NewPackageImport.ClassPackage = *ClassPath.GetLongPackageName();
		NewPackageImport.ClassName = *ClassPath.GetAssetName();
		NewPackageImport.ObjectName = PackageName;

		ImportedPackageIndex = FPackageIndex::FromImport( ImportIndex );
	}
	return ImportedPackageIndex;
}

FPackageIndex FCookedAssetWriter::CreateScriptObjectImport(const FPackageObjectIndex& PackageObjectIndex, FAssetSerializationContext& Context) const
{
	FPackageMapScriptObjectEntry ScriptObjectEntry;
	check( PackageMap->FindScriptObject( PackageObjectIndex, ScriptObjectEntry ) );
		
	// If the outer index is null, we are making a top level UPackage import
	if ( ScriptObjectEntry.OuterIndex.IsNull() )
	{
		return CreatePackageImport( ScriptObjectEntry.ObjectName, Context );
	}

	// Otherwise we should have a valid outer, and we need to resolve it first
	const FPackageIndex OuterObjectIndex = CreateScriptObjectImport( ScriptObjectEntry.OuterIndex, Context );
	FPackageIndex ResultObjectIndex = FindExistingObjectImport( OuterObjectIndex, ScriptObjectEntry.ObjectName, Context );

	// We couldn't find it, need to create one
	if ( ResultObjectIndex.IsNull() )
	{
		const int32 ImportIndex = Context.ImportMap.AddDefaulted();
		FObjectImport& NewObjectImport = Context.ImportMap[ ImportIndex ];

		// Guessing the ScriptObject Class is a bit difficult for non-top-level objects, as they can be UClass, UFunction, UEnum or UScriptStruct
		// If this is the CDO though, we know that it's Class is the ScriptObject specified in the CDO index
		if ( !ScriptObjectEntry.CDOClassIndex.IsNull() )
		{
			const FPackageIndex CDOClassPackageIndex = CreateScriptObjectImport( ScriptObjectEntry.CDOClassIndex, Context );

			const FSoftObjectPath ClassPath = ResolvePackagePath( CDOClassPackageIndex, Context ).GetAssetPathString();
			NewObjectImport.ClassName = *ClassPath.GetAssetName();
			NewObjectImport.ClassPackage = *ClassPath.GetLongPackageName();
		}
		// We know nothing about the object otherwise, can be a top level object, can be a default sub-object of some native object
		else
		{
			NewObjectImport.ClassName = "Class";
			NewObjectImport.ClassPackage = "/Script/CoreUObject";
		}
	
		NewObjectImport.OuterIndex = OuterObjectIndex;
		NewObjectImport.ObjectName = ScriptObjectEntry.ObjectName;
	
		ResultObjectIndex = FPackageIndex::FromImport( ImportIndex );
	}
	return ResultObjectIndex;
}

FPackageIndex FCookedAssetWriter::CreateExternalPackageObjectReference(const FPackageObjectIndex& PackageExportIndex, FAssetSerializationContext& Context) const
{
	int32 ExportIndex = INDEX_NONE;

	for (int32 i = 0; i < Context.BundleData->ExportMap.Num(); i++)
	{
		if (Context.BundleData->ExportMap[i].GlobalImportIndex == FIoStorePackageMap::ResolvePackageLocalRef(PackageExportIndex))
		{
			ExportIndex = i;
			break;
		}
	}

	if (ExportIndex == INDEX_NONE)
	{
		FPackageMapExportBundleEntry ImportedPackageBundle;
		if (!PackageMap->FindExportBundleData(PackageExportIndex, ImportedPackageBundle)) return FPackageIndex();

		// Find the index of the export with the specified hash
		for (int32 i = 0; i < ImportedPackageBundle.ExportMap.Num(); i++)
		{
			if (ImportedPackageBundle.ExportMap[i].GlobalImportIndex == FIoStorePackageMap::ResolvePackageLocalRef(PackageExportIndex))
			{
				ExportIndex = i;
				break;
			}
		}

		check(ExportIndex != INDEX_NONE);

		// Call the internal function that will recursively populate exports
		return CreatePackageExportReference(&ImportedPackageBundle, ExportIndex, Context);
	}

	check(ExportIndex != INDEX_NONE);
	return FPackageIndex::FromExport(ExportIndex);
}

FPackageIndex FCookedAssetWriter::CreateExternalPackageReference(const FPackageId& PackageId, FAssetSerializationContext& Context) const
{
	// Make sure to check that this is not our own export first
	if ( PackageId != Context.PackageId )
	{
		// Resolve exported package bundle first
		FPackageMapExportBundleEntry ImportedPackageBundle;
		if (!PackageMap->FindExportBundleData( PackageId, ImportedPackageBundle)) return FPackageIndex();

		return CreatePackageImport( ImportedPackageBundle.PackageName, Context );
	}

	// Reference to the current package itself
	return FPackageIndex();
}

FPackageIndex FCookedAssetWriter::ResolvePackageLocalRef( const FPackageMapExportBundleEntry* ExternalPackageData, const FPackageLocalObjectRef& ObjectRef, FAssetSerializationContext& Context ) const
{
	// If our object is an imported package, resolve the import
	if ( ObjectRef.bIsImport )
	{
		const FPackageMapImportEntry& ImportedObject = ObjectRef.Import;

		// Reference to the native script object
		if ( ImportedObject.bIsScriptImport )
		{
			return CreateScriptObjectImport( ImportedObject.ScriptImportIndex, Context );
		}
		// Reference to the object inside of the another package
		if ( ImportedObject.bIsPackageImport )
		{
			return CreateExternalPackageObjectReference( ImportedObject.GlobalImportIndex, Context );
		}
	}
	// Reference to an export inside of the current package
	else if ( ObjectRef.bIsExportReference )
	{
		return CreatePackageExportReference( ExternalPackageData, ObjectRef.ExportIndex, Context );
	}

	// Otherwise this is Null. Meaning of null depends on the context in which local reference is being deserialized
	check( ObjectRef.bIsNull || ObjectRef.Import.bIsNullImport );
	
	// If we are resolving a reference in the scope of the external package, this would always be a reference to the external package as an import
	if ( ExternalPackageData != nullptr )
	{
		return CreatePackageImport( ExternalPackageData->PackageName, Context );
	}
	// Otherwise it is either a plain Null or a reference to the current package. The differentiation is on the loader, for us it's an empty package index regardless
	return FPackageIndex();
}

FPackageIndex FCookedAssetWriter::CreatePackageExportReference( const FPackageMapExportBundleEntry* ExternalPackageData, int32 ExportIndex, FAssetSerializationContext& Context) const
{
	// Only attempt to resolve package data if this is an external package we are attempting to import
	if ( ExternalPackageData != nullptr && ExternalPackageData->PackageName != Context.BundleData->PackageName )
	{
		const FPackageMapExportEntry& ExportData = ExternalPackageData->ExportMap[ ExportIndex ];
	
		// Resolve the outer for the exported object first
		const FPackageIndex OuterIndex = ResolvePackageLocalRef( ExternalPackageData, ExportData.OuterIndex, Context );

		// Attempt to find the existing import first
		FPackageIndex ResultIndex = FindExistingObjectImport( OuterIndex, ExportData.ObjectName, Context );

		// Need to create it if it does not already exist
		if ( ResultIndex.IsNull() )
		{
			const int32 ImportIndex = Context.ImportMap.AddDefaulted();
			FObjectImport& NewObjectImport = Context.ImportMap[ ImportIndex ];

			// The class name might be one of our exports in case of circular dependencies (which is the point),
			// so we need to postpone class name fixup for this import until we have written our exports
			const FPackageIndex ExportClassIndex = ResolvePackageLocalRef( ExternalPackageData, ExportData.ClassIndex, Context );
		
			Context.ImportClassPathFixup.Add( ImportIndex, ExportClassIndex );
			NewObjectImport.OuterIndex = OuterIndex;
			NewObjectImport.ObjectName = ExportData.ObjectName;

			ResultIndex = FPackageIndex::FromImport( ImportIndex );
		}
		return ResultIndex;
	}
	
	// Otherwise this is a reference to the export of the currently serialized package
	return FPackageIndex::FromExport( ExportIndex );
}

FSoftObjectPath FCookedAssetWriter::ResolvePackagePath( FPackageIndex PackageIndex, FAssetSerializationContext& Context )
{
	// Collect the asset path
	TArray<FName> TotalAssetPath;
	FPackageIndex CurrentPackageIndex = PackageIndex;

	while ( !CurrentPackageIndex.IsNull() )
	{
		if ( CurrentPackageIndex.IsImport() )
		{
			const FObjectResource& PackageResource = Context.ImportMap[ CurrentPackageIndex.ToImport() ];
			TotalAssetPath.Add( PackageResource.ObjectName );
			CurrentPackageIndex = PackageResource.OuterIndex;
		}
		else if ( CurrentPackageIndex.IsExport() )
		{
			const FObjectResource& PackageResource = Context.ExportMap[ CurrentPackageIndex.ToExport() ];
			TotalAssetPath.Add( PackageResource.ObjectName );
			CurrentPackageIndex = PackageResource.OuterIndex;

			// If outer index is Null, we need to manually append package name of the currently exported package
			if ( CurrentPackageIndex.IsNull() )
			{
				TotalAssetPath.Add( Context.BundleData->PackageName );
			}
		}
	}

	// Flip the asset path to the Outermost.Outer:Inner
	Algo::Reverse( TotalAssetPath );

	// Build top level asset path
	const FName PackageName = TotalAssetPath.IsValidIndex( 0 ) ? TotalAssetPath[ 0 ] : NAME_None;
	const FName TopLevelAssetName = TotalAssetPath.IsValidIndex( 1 ) ? TotalAssetPath[ 1 ] : NAME_None;

	// Build sub-object path, starting at index 2 (0 is package name, 1 is asset name)
	TStringBuilder<128> SubObjectPathBuilder;
	for ( int32 i = 2; i < TotalAssetPath.Num(); i++ )
	{
		if ( i != 2 )
		{
			SubObjectPathBuilder.Append( SUBOBJECT_DELIMITER_CHAR );
		}
		TotalAssetPath[ i ].ToString( SubObjectPathBuilder );
	}

	// Finally construct the soft object path
	return FSoftObjectPath( FName( PackageName.ToString() + '.' + TopLevelAssetName.ToString()), SubObjectPathBuilder.ToString());
}

void FExportPreloadDependencyList::AddDependency( uint32 CurrentCommand, FPackageIndex FromIndex, uint32 FromCommand )
{
	if ( FromIndex != OwnerIndex && !FromIndex.IsNull() )
	{
		if ( CurrentCommand == FExportBundleEntry::ExportCommandType_Create )
		{
			if ( FromCommand == FExportBundleEntry::ExportCommandType_Create )
			{
				CreateBeforeCreateDependencies.AddUnique( FromIndex );
			}
			else if ( FromCommand == FExportBundleEntry::ExportCommandType_Serialize )
			{
				SerializeBeforeCreateDependencies.AddUnique( FromIndex );
			}
		}
		else if ( CurrentCommand == FExportBundleEntry::ExportCommandType_Serialize )
		{
			if ( FromCommand == FExportBundleEntry::ExportCommandType_Create )
			{
				CreateBeforeSerializeDependencies.AddUnique( FromIndex );
			}
			else if ( FromCommand == FExportBundleEntry::ExportCommandType_Serialize )
			{
				SerializeBeforeSerializeDependencies.AddUnique( FromIndex );
			}
		}
	}
}

FExportBundleEntry FCookedAssetWriter::BuildPreloadDependenciesFromExportBundle( int32 ExportBundleIndex, FAssetSerializationContext& Context )
{
	const TArray<FExportBundleEntry>& ExportBundle = Context.BundleData->ExportBundles[ ExportBundleIndex ];
	
	// Only attempt to build the bundle if we have not done it before (this might be called multiple times in case bundles depend on each other)
	if ( !Context.ProcessedExportBundles.Contains( ExportBundleIndex ) )
	{
		Context.ProcessedExportBundles.Add( ExportBundleIndex );
		const FExportBundleEntry& FirstExportInBundle = ExportBundle[ 0 ];
    	FExportPreloadDependencyList& FirstPreloadDependency = Context.PreloadDependencies[ FirstExportInBundle.LocalExportIndex ];
    
    	// Add external dependencies to the first export in the bundle
    	for ( const FPackageMapExternalDependencyArc& ExternalDependency : Context.BundleData->ExternalArcs )
    	{
    		for (const FArc& ExternalArc : ExternalDependency.Arcs )
    		{
    			if ( ExternalArc.ToNodeIndex == ExportBundleIndex && (int32)ExternalArc.FromNodeIndex >= 0 )
    			{
    				const FPackageIndex ImportIndex = FPackageIndex::FromImport( ExternalArc.FromNodeIndex );
    				FirstPreloadDependency.AddDependency( FirstExportInBundle.CommandType, ImportIndex, FirstExportInBundle.CommandType );
    			}
    		}
    	}
    
    	// Go over the exports in the bundle in their order and add dependency on the previous one for each export
    	for ( int32 i = 1; i < ExportBundle.Num(); i++ )
    	{
    		const FExportBundleEntry& PreviousExportInBundle = ExportBundle[ i - 1 ];
    		const FExportBundleEntry& CurrentExport = ExportBundle[ i ];
    
    		const FPackageIndex& ExportIndex = FPackageIndex::FromExport( PreviousExportInBundle.LocalExportIndex );
    		FExportPreloadDependencyList& CurrentPreloadDependency = Context.PreloadDependencies[ CurrentExport.LocalExportIndex ];
    		CurrentPreloadDependency.AddDependency( CurrentExport.CommandType, ExportIndex, PreviousExportInBundle.CommandType );
    	}
	}

	// Return the last export in the export bundle on which the dependent bundles can depend
	return ExportBundle.Last();
}

void FCookedAssetWriter::BuildPreloadDependenciesFromArcs(FAssetSerializationContext& Context)
{
	// Setup preload dependencies with the export sizes
	for ( int32 ExportIndex = 0; ExportIndex < Context.ExportMap.Num(); ExportIndex++ )
	{
		FExportPreloadDependencyList& PreloadDependencyList = Context.PreloadDependencies.AddDefaulted_GetRef();
		PreloadDependencyList.OwnerIndex = FPackageIndex::FromExport( ExportIndex );
	}
	
	// Build export bundles in their order of definition
	for ( int32 ExportBundleIndex = 0; ExportBundleIndex < Context.BundleData->ExportBundles.Num(); ExportBundleIndex++ )
	{
		BuildPreloadDependenciesFromExportBundle( ExportBundleIndex, Context );
	}

	// Append additional dependencies from the exports
	BuildPreloadDependenciesFromExports( Context );
}

void FCookedAssetWriter::BuildPreloadDependenciesFromExports(FAssetSerializationContext& Context)
{
	for ( int32 ExportIndex = 0; ExportIndex < Context.ExportMap.Num(); ExportIndex++ )
	{
		const FObjectExport& Export = Context.ExportMap[ ExportIndex ];
		FExportPreloadDependencyList& ExportDependencies = Context.PreloadDependencies[ ExportIndex ];

		// SerializationBeforeCreateDependencies should be ClassIndex and TemplateIndex
		ExportDependencies.AddDependency( FExportBundleEntry::ExportCommandType_Create, Export.ClassIndex, FExportBundleEntry::ExportCommandType_Serialize );
		ExportDependencies.AddDependency( FExportBundleEntry::ExportCommandType_Create, Export.TemplateIndex, FExportBundleEntry::ExportCommandType_Serialize );

		// CreateBeforeCreateDependencies should be OuterIndex and SuperIndex
		ExportDependencies.AddDependency( FExportBundleEntry::ExportCommandType_Create, Export.OuterIndex, FExportBundleEntry::ExportCommandType_Create );
		ExportDependencies.AddDependency( FExportBundleEntry::ExportCommandType_Create, Export.SuperIndex, FExportBundleEntry::ExportCommandType_Create );
	}
}

void FCookedAssetWriter::ReorderPackageImports(const TArray<int32>& OriginalImportOrder, FAssetSerializationContext& Context)
{
	// Initialize the index map with prebuilt indices
	TArray<int32> OldIndexToNewIndexMap;
	TArray<int32> NewIndexToOldIndexMap;
	TBitArray<> FilledIndices;
	
	OldIndexToNewIndexMap.AddUninitialized( Context.ImportMap.Num() );
	NewIndexToOldIndexMap.AddUninitialized( OldIndexToNewIndexMap.Num() );
	FilledIndices.Init( false, OldIndexToNewIndexMap.Num() );

	for ( int32 NewIndex = 0; NewIndex < OriginalImportOrder.Num(); NewIndex++ )
	{
		const int32 OriginalIndex = OriginalImportOrder[ NewIndex ];

		OldIndexToNewIndexMap[ OriginalIndex ] = NewIndex;
		NewIndexToOldIndexMap[ NewIndex ] = OriginalIndex;
		FilledIndices[ OriginalIndex ] = true;
	}

	// Fill in the rest of the indices in their original order
	int32 NextFreeImportIndex = OriginalImportOrder.Num();
	for ( int32 OldIndex = 0; OldIndex < Context.ImportMap.Num(); OldIndex++ )
	{
		if ( !FilledIndices[ OldIndex ] )
		{
			const int32 NewIndex = NextFreeImportIndex++;
			
			OldIndexToNewIndexMap[ OldIndex ] = NewIndex;
			NewIndexToOldIndexMap[ NewIndex ] = OldIndex;
			FilledIndices[ OldIndex ] = true;
		}
	}

	// Build the new import table with the indices remapped
	TArray<FObjectImport> NewImports;

	for ( int32 NewIndex = 0; NewIndex < Context.ImportMap.Num(); NewIndex++ )
	{
		const int32 OldIndex = NewIndexToOldIndexMap[ NewIndex ];
		const FObjectImport& OldImport = Context.ImportMap[ OldIndex ];
		FObjectImport& NewImport = NewImports.AddDefaulted_GetRef();

		NewImport.ClassName = OldImport.ClassName;
		NewImport.ClassPackage = OldImport.ClassPackage;
		NewImport.OuterIndex = OldImport.OuterIndex.IsImport() ? FPackageIndex::FromImport( OldIndexToNewIndexMap[ OldImport.OuterIndex.ToImport() ] ) : OldImport.OuterIndex;
		NewImport.ObjectName = OldImport.ObjectName;
	}
	Context.ImportMap = NewImports;

	// Rebuild import fixup map
	TMap<int32, FPackageIndex> ObjectFixupMap;

	for ( const TPair<int32, FPackageIndex>& Pair : Context.ImportClassPathFixup )
	{
		const int32 OldIndex = Pair.Key;
		const FPackageIndex OldIndexValue = Pair.Value;

		const int32 NewIndex = OldIndexToNewIndexMap[ OldIndex ];
		const FPackageIndex NewIndexValue = OldIndexValue.IsImport() ? FPackageIndex::FromImport( OldIndexToNewIndexMap[ OldIndexValue.ToImport() ] ) : OldIndexValue;
		ObjectFixupMap.Add( NewIndex, NewIndexValue );
	}
	Context.ImportClassPathFixup = ObjectFixupMap;
}

FPackageIndex FCookedAssetWriter::CreateObjectExport( const FPackageMapExportEntry& ExportData, FAssetSerializationContext& Context ) const
{
	const int32 NewExportIndex = Context.ExportMap.AddDefaulted();
	FObjectExport& ObjectExport = Context.ExportMap[ NewExportIndex ];

	ObjectExport.ClassIndex = ResolvePackageLocalRef( nullptr, ExportData.ClassIndex, Context );
	ObjectExport.SuperIndex = ResolvePackageLocalRef( nullptr, ExportData.SuperIndex, Context );
	ObjectExport.TemplateIndex = ResolvePackageLocalRef( nullptr, ExportData.TemplateIndex, Context );

	ObjectExport.OuterIndex = ResolvePackageLocalRef( nullptr, ExportData.OuterIndex, Context );
	ObjectExport.ObjectName = ExportData.ObjectName;

	ObjectExport.ObjectFlags = ExportData.ObjectFlags;
	
	ObjectExport.SerialSize = INDEX_NONE;
	ObjectExport.SerialOffset = INDEX_NONE;

	ObjectExport.bForcedExport = false; // not serialized
	ObjectExport.bNotForClient = EnumHasAnyFlags( ExportData.FilterFlags, EExportFilterFlags::NotForClient );
	ObjectExport.bNotForServer = EnumHasAnyFlags( ExportData.FilterFlags, EExportFilterFlags::NotForServer );

	ObjectExport.bNotAlwaysLoadedForEditorGame = false; // not serialized

	// Not serialized, but assume that any public exports are assets, and as of UE5 this is true for everything, including the BPGC
	// The logic is indirectly copied from UObject::IsAsset, which is usually not overriden for anything
	ObjectExport.bIsAsset = ( ObjectExport.ObjectFlags & ( RF_ClassDefaultObject | RF_ArchetypeObject ) ) == 0 && ( ObjectExport.ObjectFlags & RF_Public ) != 0 && ObjectExport.OuterIndex.IsNull();

	// Doesn't look like it is relevant for basically anything, but it's not serialized, so we derive the flags from the current package
	// Make sure to also clean up more uncommon flags that are probably not set on the imported package
	ObjectExport.PackageFlags = Context.BundleData->PackageFlags & ~( PKG_ContainsMap | PKG_ContainsMapData | PKG_ContainsNoAsset | PKG_DynamicImports );

	// Set this to INDEX_NONE by default, it will be overwritten later when applicable
	ObjectExport.FirstExportDependency = INDEX_NONE;

	return FPackageIndex::FromExport( NewExportIndex );
}

void FCookedAssetWriter::ProcessPackageSummaryAndNamesAndExportsAndImports( FAssetSerializationContext& Context ) const
{
	FPackageFileSummary& Summary = Context.Summary;

	// Serialize general data
	Summary.Tag = PACKAGE_FILE_TAG;
	Summary.PackageFlags = Context.BundleData->PackageFlags;
	reinterpret_cast<FUglyPackageSummaryPackageFlagsAccessWorkaround*>( &Summary )->PackageFlags = Context.BundleData->PackageFlags;

	Summary.FolderName = "None";

	Summary.SetFileVersions(GPackageFileUE4Version, GPackageFileLicenseeUE4Version, true);

	// Clone name map into the Context
	for ( const FName& NameMapName : Context.BundleData->NameMap )
	{
		check( NameMapName.GetNumber() == NAME_NO_NUMBER_INTERNAL );
		
		const int32 NameIndex = Context.NameMap.Add( NameMapName );
		Context.NameReverseLookupMap.Add( NameMapName, NameIndex );
	}
	Summary.NameCount = Context.BundleData->NameMap.Num();

	// Read package header because we need it to re-hydrate our imports
	FPackageHeaderData PackageHeaderData;
	check( PackageMap->FindPackageHeader( Context.PackageId, PackageHeaderData ) );

	// Resolve import entries from the bundle
	int32 CurrentImportedPackageIndex = 0;
	TArray<int32> OriginalImportOrder;
	
	for ( const FPackageMapImportEntry& ImportMapEntry : Context.BundleData->ImportMap )
	{
		// Resolve script import first
		if ( ImportMapEntry.bIsScriptImport )
		{
			const FPackageIndex TopmostImportIndex = CreateScriptObjectImport( ImportMapEntry.ScriptImportIndex, Context );
			OriginalImportOrder.Add( TopmostImportIndex.ToImport() );
		}
		// Otherwise attempt to resolve package import
		else if ( ImportMapEntry.bIsPackageImport )
		{
			const FPackageIndex TopmostImportIndex = CreateExternalPackageObjectReference( ImportMapEntry.GlobalImportIndex, Context );
			if (!TopmostImportIndex.IsNull()) OriginalImportOrder.Add( TopmostImportIndex.ToImport() );
		}
		// Otherwise it is a null import
		// Null imports are the remnants of the top level package imports after they have been pre-processed by Zen,
		// so we try to preserve them and re-hydrate them with package imports we found
		else
		{
			const FPackageId ImportedPackageId = PackageHeaderData.ImportedPackages[ CurrentImportedPackageIndex++ ];
			const FPackageIndex PackageImportIndex = CreateExternalPackageReference( ImportedPackageId, Context );
			if (!PackageImportIndex.IsNull()) OriginalImportOrder.Add( PackageImportIndex.ToImport() );
		}
	}

	// Re-order imports according to the original imports map
	ReorderPackageImports( OriginalImportOrder, Context );

	// Resolve export entries from the bundle
	for ( const FPackageMapExportEntry& ExportMapEntry : Context.BundleData->ExportMap )
	{
		CreateObjectExport( ExportMapEntry, Context );
	}

	// Apply late import class path fix-ups
	for ( const TPair<int32, FPackageIndex>& ImportClassPathPair : Context.ImportClassPathFixup )
	{
		FObjectImport& ObjectImport = Context.ImportMap[ ImportClassPathPair.Key ];
		const FSoftObjectPath& ClassPath = ResolvePackagePath( ImportClassPathPair.Value, Context ).GetAssetPathString();

		ObjectImport.ClassName = *ClassPath.GetAssetName();
		ObjectImport.ClassPackage = *ClassPath.GetLongPackageName();
	}
	
	Summary.ExportCount = Context.ExportMap.Num();
	Summary.ImportCount = Context.ImportMap.Num();

	// Build preload dependencies map
	BuildPreloadDependenciesFromArcs( Context );
}

void FCookedAssetWriter::WritePackageHeader(FArchive& Ar, FAssetSerializationContext& Context)
{
	check( Context.Summary.PackageFlags & PKG_FilterEditorOnly );
	
	// Collect NameMap references from import and export map before we attempt to serialize them
	{
		FArchive dummyArchive{};
		FAssetSerializationWriter nameMapCollector{ dummyArchive, &Context };

		for ( FObjectImport& Import : Context.ImportMap )
		{
			nameMapCollector << Import;
		}
		for ( FObjectExport& Export : Context.ExportMap )
		{
			nameMapCollector << Export;
		}
	}

	// Write dummy generation info for current generation
	{
		FGenerationInfo& GenerationInfo = Context.Summary.Generations.AddZeroed_GetRef();
		GenerationInfo.ExportCount = Context.ExportMap.Num();
		GenerationInfo.NameCount = Context.NameMap.Num();
	}
	
	// Write dummy package summary that we will patch up later
	Ar << Context.Summary;
	Context.PackageSummaryEndOffset = Ar.Tell();
	
	// Write Name Map
	Context.Summary.NameOffset = Context.PackageSummaryEndOffset;
	Context.Summary.NameCount = Context.NameMap.Num();
	{
		TGuardValue<bool> WritingNameMapGuard( Context.bSerializingNameMap, true );
		
		for ( FName& NameMapEntry : Context.NameMap )
		{
			Ar << NameMapEntry;
		}
	}
	// We cannot add new names to the map after this point
	Context.bNameMapWrittenToFile = true;

	// Soft Object Paths are not present in the cooked assets
	// GatherableText are not present in the cooked assets

	// Save Import Map
	{
		Context.Summary.ImportOffset = (int32) Ar.Tell();
		for ( FObjectImport& Import : Context.ImportMap )
		{
			Ar << Import;
		}
	}

	// Save Dummy Export Map
	Context.ExportMapStartOffset = Ar.Tell();
	{
		Context.Summary.ExportOffset = (int32) Ar.Tell();
		for ( FObjectExport& Export : Context.ExportMap )
		{
			Ar << Export;
		}
	}

	// Save Dummy Depend Map, not populated for cooked packages
	{
		Context.Summary.DependsOffset = (int32) Ar.Tell();

		TArray<FPackageIndex> Depends; // empty array
		for (int32 ExportIndex = 0; ExportIndex < Context.ExportMap.Num(); ++ExportIndex)
		{
			Ar << Depends;
		}
	}

	// Filter out editor only data from the package summary
	Context.Summary.SoftPackageReferencesCount = 0;
	Context.Summary.SoftPackageReferencesOffset = 0;
	Context.Summary.SearchableNamesOffset = 0;

	// Thumbnails are not written for cooked packages
	Context.Summary.ThumbnailTableOffset = 0;
	
	// Asset registry data is filtered out for cooked packages
	Context.Summary.AssetRegistryDataOffset = (int32) Ar.Tell();
	{
		int32 DummyAssetObjectCount = 0;
		Ar << DummyAssetObjectCount;
	}
	
	// Legacy World Composition information, we do not have a way to obtain it and it is not used
	Context.Summary.WorldTileInfoDataOffset = 0;

	// Write Preload Dependencies
	Context.Summary.PreloadDependencyOffset = (int32) Ar.Tell();
	{
		Context.Summary.PreloadDependencyCount = 0;

		for ( int32 i = 0; i < Context.PreloadDependencies.Num(); i++ )
		{
			FExportPreloadDependencyList& PreloadDependency = Context.PreloadDependencies[ i ];
			FObjectExport& ObjectExport = Context.ExportMap[ i ];

			// Set the dependency counts on the export
			ObjectExport.FirstExportDependency = Context.Summary.PreloadDependencyCount;
			ObjectExport.SerializationBeforeSerializationDependencies = PreloadDependency.SerializeBeforeSerializeDependencies.Num();
			ObjectExport.CreateBeforeSerializationDependencies = PreloadDependency.CreateBeforeSerializeDependencies.Num();
			ObjectExport.SerializationBeforeCreateDependencies = PreloadDependency.SerializeBeforeCreateDependencies.Num();
			ObjectExport.CreateBeforeCreateDependencies = PreloadDependency.CreateBeforeCreateDependencies.Num();

			Context.Summary.PreloadDependencyCount += ObjectExport.SerializationBeforeSerializationDependencies +
				ObjectExport.CreateBeforeSerializationDependencies + ObjectExport.SerializationBeforeCreateDependencies + ObjectExport.CreateBeforeCreateDependencies;
			
			// Write the actual dependencies into the archive
			for ( FPackageIndex& PackageIndex : PreloadDependency.SerializeBeforeSerializeDependencies )
			{
				Ar << PackageIndex;
			}
			for ( FPackageIndex& PackageIndex : PreloadDependency.CreateBeforeSerializeDependencies )
			{
				Ar << PackageIndex;
			}
			for ( FPackageIndex& PackageIndex : PreloadDependency.SerializeBeforeCreateDependencies )
			{
				Ar << PackageIndex;
			}
			for ( FPackageIndex& PackageIndex : PreloadDependency.CreateBeforeCreateDependencies )
			{
				Ar << PackageIndex;
			}
		}
	}

	// Update total header size
	Context.Summary.TotalHeaderSize = (int32) Ar.Tell();
	// Add TotalHeaderSize to the BulkDataStartOffset
	Context.Summary.BulkDataStartOffset += Context.Summary.TotalHeaderSize;

	// Fixup SerialOffset in ExportMap to take header size into account
	{
		const int64 OffsetBeforeSeek = Ar.Tell();
		Ar.Seek( Context.ExportMapStartOffset );

		for ( FObjectExport& Export : Context.ExportMap )
		{
			Export.SerialOffset += Context.Summary.TotalHeaderSize;
			Ar << Export;
		}
		Ar.Seek( OffsetBeforeSeek );
	}

	// Write the finalized package header
	{
		const int64 OffsetBeforeSeek = Ar.Tell();
		Ar.Seek( 0 );
		Ar << Context.Summary;
		Ar.Seek( OffsetBeforeSeek );
	}
}

void FCookedAssetWriter::WritePackageExports(FArchive& Ar, FAssetSerializationContext& Context)
{
	// Open the package bundle chunk to read exports
	TIoStatusOr<FIoBuffer> ChunkBuffer = Context.IoStoreReader->Read( Context.BundleData->PackageChunkId, FIoReadOptions() );
	check( ChunkBuffer.IsOk() );
	const uint8* ChunkDataStart = ChunkBuffer.ValueOrDie().Data();
	const uint8* ChunkDataEnd = ChunkDataStart + ChunkBuffer.ValueOrDie().DataSize();
	
	// Write export blobs
	for ( int32 i = 0; i < Context.ExportMap.Num(); i++ )
	{
		const FPackageMapExportEntry& OriginalExport = Context.BundleData->ExportMap[ i ];
		FObjectExport& Export = Context.ExportMap[ i ];

		Export.SerialOffset = Ar.Tell();
		Export.SerialSize = OriginalExport.SerialDataSize;

		const uint8* SerialDataStart = ChunkDataStart + OriginalExport.SerialDataOffset;
		check( SerialDataStart <= ChunkDataEnd );
		Ar.Serialize( const_cast<uint8*>( SerialDataStart ), OriginalExport.SerialDataSize );
	}
	Context.Summary.BulkDataStartOffset = Ar.Tell();

	// Exports end with the package file tag
	uint32 FooterData = PACKAGE_FILE_TAG;
	Ar << FooterData;
}

void FCookedAssetWriter::WriteBulkData( const FAssetSerializationContext& Context )
{
	FSavedPackageInfo& SavedPackageInfo = SavedPackageMap.FindOrAdd( Context.BundleData->PackageName );
	
	for ( const FIoChunkId& BulkDataChunkId : Context.BundleData->BulkDataChunkIds )
	{
		TIoStatusOr<FIoBuffer> BulkDataBuffer = Context.IoStoreReader->Read( BulkDataChunkId, FIoReadOptions() );
		check( BulkDataBuffer.IsOk() );

		TIoStatusOr<FIoStoreTocChunkInfo> ChunkInfo = Context.IoStoreReader->GetChunkInfo( BulkDataChunkId );
		check( ChunkInfo.IsOk() );

		FString RelativeFilename = Context.BundleData->PackageName.ToString();
		RelativeFilename.RemoveFromStart( TEXT("../../../") );

		const FString ResultFilename = FPaths::Combine( RootOutputDir, RelativeFilename + ".ubulk");
		FFileHelper::SaveArrayToFile( TArrayView<const uint8>( BulkDataBuffer.ValueOrDie().Data(), BulkDataBuffer.ValueOrDie().DataSize() ), *ResultFilename );

		ChunkIdToSavedFileMap.Add( BulkDataChunkId, RelativeFilename );
		SavedPackageInfo.BulkDataChunks.Add( BulkDataChunkId );
	}
}
