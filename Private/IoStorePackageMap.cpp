// Copyright Nikita Zolotukhin. All Rights Reserved.

#include "IoStorePackageMap.h"

#include "ZenTools.h"
#include "Serialization/LargeMemoryReader.h"
#include "Serialization/MemoryReader.h"
#include "Misc/KeyChainUtilities.h"
#include "UObject/NameBatchSerialization.h"

void FIoStorePackageMap::PopulateFromContainer(const TSharedPtr<FIoStoreReader>& Reader)
{
	bool bReadScriptObjects = true;
	// If this is a global container, read the Script Objects from it
	TIoStatusOr<FIoBuffer> NamesIoBuffer = Reader->Read(CreateIoChunkId(0, 0, EIoChunkType::LoaderGlobalNames), FIoReadOptions());
	if (!NamesIoBuffer.IsOk())
	{
		bReadScriptObjects = false;
	}
	
	TIoStatusOr<FIoBuffer> NameHashesIoBuffer = Reader->Read(CreateIoChunkId(0, 0, EIoChunkType::LoaderGlobalNameHashes), FIoReadOptions());
	if (!NameHashesIoBuffer.IsOk())
	{
		bReadScriptObjects = false;
	}	

	TIoStatusOr<FIoBuffer> InitialLoadIoBuffer = Reader->Read(CreateIoChunkId(0, 0, EIoChunkType::LoaderInitialLoadMeta), FIoReadOptions());
	if (!InitialLoadIoBuffer.IsOk())
	{
		bReadScriptObjects = false;
	}

	if (bReadScriptObjects)
		ReadScriptObjects( InitialLoadIoBuffer.ValueOrDie(), NamesIoBuffer.ValueOrDie(), NameHashesIoBuffer.ValueOrDie() );
	
	TArray<FPackageId> PackageIdsInThisContainer;
	TArray<FPackageId> OptionalPackageIdsInThisContainer;
	
	// Read the Package Headers from the Container Header of the container.
	TIoStatusOr<FIoBuffer> ContainerHeaderBuffer = Reader->Read(CreateIoChunkId(Reader->GetContainerId().Value(), 0, EIoChunkType::ContainerHeader), FIoReadOptions());
	if (ContainerHeaderBuffer.IsOk())
	{
		FMemoryReaderView Ar(MakeArrayView(ContainerHeaderBuffer.ValueOrDie().Data(), ContainerHeaderBuffer.ValueOrDie().DataSize()));
		FContainerHeader ContainerHeader;
		Ar << ContainerHeader;

		TArrayView<FPackageStoreEntry> StoreEntries(reinterpret_cast<FPackageStoreEntry*>(ContainerHeader.StoreEntries.GetData()), ContainerHeader.PackageIds.Num());

		int32 PackageIndex = 0;
		for (FPackageStoreEntry& ContainerEntry : StoreEntries)
		{
			const FPackageId& PackageId = ContainerHeader.PackageIds[PackageIndex++];
			FPackageHeaderData& PackageHeader = PackageHeaders.FindOrAdd(PackageId);
			
			PackageHeader.ImportedPackages = TArrayView<FPackageId>(ContainerEntry.ImportedPackages.Data(), ContainerEntry.ImportedPackages.Num());
			PackageHeader.ExportCount = ContainerEntry.ExportCount;
			PackageHeader.ExportBundleCount = ContainerEntry.ExportBundleCount;
			
			PackageIdsInThisContainer.Add(PackageId);
		}
	}

	// Iterate package chunks from the header
	for ( const FPackageId& PackageId : PackageIdsInThisContainer )
	{
		// Optional chunk has index 1, required one has index 0
		const FIoChunkId ChunkId = CreateIoChunkId( PackageId.Value(), 0, EIoChunkType::ExportBundleData );
		
		TIoStatusOr<FIoStoreTocChunkInfo> ChunkInfo = Reader->GetChunkInfo( ChunkId );
		TIoStatusOr<FIoBuffer> PackageBuffer = Reader->Read( ChunkId, FIoReadOptions() );
		check( PackageBuffer.IsOk() );

		FPackageMapExportBundleEntry* ExportBundleEntry = ReadExportBundleData( PackageId, ChunkInfo.ValueOrDie(), PackageBuffer.ValueOrDie() );

		// Required segment packages can have bulk data, memory mapped bulk data and optional bulk data
		TArray<EIoChunkType> BulkDataChunkTypes;
		BulkDataChunkTypes.Add(EIoChunkType::BulkData);
		BulkDataChunkTypes.Add(EIoChunkType::MemoryMappedBulkData);
		BulkDataChunkTypes.Add(EIoChunkType::OptionalBulkData);

		for ( const EIoChunkType BulkDataChunkType : BulkDataChunkTypes )
		{
			const FIoChunkId BulkDataChunkId = CreateIoChunkId( PackageId.Value(), 0, BulkDataChunkType );
			if ( Reader->GetChunkInfo( BulkDataChunkId ).IsOk() )
			{
				ExportBundleEntry->BulkDataChunkIds.Add( BulkDataChunkId );
			}
		}
	}

	// Iterate optional packages from the header
	for ( const FPackageId& PackageId : OptionalPackageIdsInThisContainer )
	{
		// Optional chunk has index 1, required one has index 0
		const FIoChunkId ChunkId = CreateIoChunkId( PackageId.Value(), 1, EIoChunkType::ExportBundleData );
		
		TIoStatusOr<FIoStoreTocChunkInfo> ChunkInfo = Reader->GetChunkInfo( ChunkId );
		TIoStatusOr<FIoBuffer> PackageBuffer = Reader->Read( ChunkId, FIoReadOptions() );
		check( PackageBuffer.IsOk() );
		
		FPackageMapExportBundleEntry* ExportBundleEntry = ReadExportBundleData( PackageId, ChunkInfo.ValueOrDie(), PackageBuffer.ValueOrDie() );

		// Optional segment packages can only have optional segment bulk data
		const FIoChunkId BulkDataChunkId = CreateIoChunkId( PackageId.Value(), 1, EIoChunkType::BulkData );
		if ( Reader->GetChunkInfo( BulkDataChunkId ).IsOk() )
		{
			ExportBundleEntry->BulkDataChunkIds.Add( BulkDataChunkId );
		}
	}

	FPackageContainerMetadata& Metadata = ContainerMetadata.FindOrAdd( Reader->GetContainerId() );

	Metadata.PackagesInContainer = PackageIdsInThisContainer;
	Metadata.OptionalPackagesInContainer = OptionalPackageIdsInThisContainer;
}

bool FIoStorePackageMap::FindPackageContainerMetadata(FIoContainerId ContainerId, FPackageContainerMetadata& OutMetadata) const
{
	if ( const FPackageContainerMetadata* Metadata = ContainerMetadata.Find( ContainerId ) )
	{
		OutMetadata = *Metadata;
		return true;
	}
	return false;
}

bool FIoStorePackageMap::FindPackageHeader(const FPackageId& PackageId, FPackageHeaderData& OutPackageHeader) const
{
	if ( const FPackageHeaderData* HeaderData = PackageHeaders.Find( PackageId ) )
	{
		OutPackageHeader = *HeaderData;
		return true;
	}
	return false;
}

bool FIoStorePackageMap::FindScriptObject(const FPackageObjectIndex& Index, FPackageMapScriptObjectEntry& OutMapEntry) const
{
	check( Index.IsScriptImport() );
	if ( const FPackageMapScriptObjectEntry* Entry = ScriptObjectMap.Find( Index ) )
	{
		OutMapEntry = *Entry;
		return true;
	}
	return false;
}

bool FIoStorePackageMap::FindExportBundleData(const FPackageId& PackageId, FPackageMapExportBundleEntry& OutExportBundleEntry) const
{
	for (const auto& PackageInfo : PackageInfos)
	{
		if (PackageId == PackageInfo.PackageId)
		{
			OutExportBundleEntry = PackageInfo;
			return true;
		}
	}
	return false;
}

bool FIoStorePackageMap::FindExportBundleData(const FPackageObjectIndex& Index,
	FPackageMapExportBundleEntry& OutExportBundleEntry) const
{
	for (const auto& PackageInfo : PackageInfos)
	{
		for (const auto& Export : PackageInfo.ExportMap)
		{
			if (ResolvePackageLocalRef(Index) == Export.GlobalImportIndex)
			{
				OutExportBundleEntry = PackageInfo;
				return true;
			}
		}
	}
	return false;
}

int32 FIoStorePackageMap::FindExportIndex(const FPackageObjectIndex& Index)
{
	return ExportIndices[Index];
}

void FIoStorePackageMap::ReadScriptObjects(const FIoBuffer& ChunkBuffer, const FIoBuffer& NamesIoBuffer, const FIoBuffer& NamesHashesIoBuffer)
{
	FLargeMemoryReader ScriptObjectsArchive(ChunkBuffer.Data(), ChunkBuffer.DataSize());
	TArray<FNameEntryId> GlobalNameMap;

	LoadNameBatch(
			GlobalNameMap,
			TArrayView<const uint8>(NamesIoBuffer.Data(), NamesIoBuffer.DataSize()),
			TArrayView<const uint8>(NamesHashesIoBuffer.Data(), NamesHashesIoBuffer.DataSize()));

	int32 NumScriptObjects = 0;
	ScriptObjectsArchive << NumScriptObjects;

	const FScriptObjectEntry* ScriptObjectEntries = reinterpret_cast<const FScriptObjectEntry*>(ChunkBuffer.Data() + ScriptObjectsArchive.Tell());

	for (int32 ScriptObjectIndex = 0; ScriptObjectIndex < NumScriptObjects; ScriptObjectIndex++)
	{
		const FScriptObjectEntry& ScriptObjectEntry = ScriptObjectEntries[ScriptObjectIndex];
		FMappedName MappedName = FMappedName::FromMinimalName(ScriptObjectEntry.ObjectName);
		check(MappedName.IsGlobal());
		
		FPackageMapScriptObjectEntry& ScriptObject = ScriptObjectMap.FindOrAdd( ScriptObjectEntry.GlobalIndex );
		ScriptObject.ScriptObjectIndex = ScriptObjectEntry.GlobalIndex;
		ScriptObject.ObjectName = FName::CreateFromDisplayId(GlobalNameMap[MappedName.GetIndex()], MappedName.GetNumber());;
		ScriptObject.OuterIndex = ScriptObjectEntry.OuterIndex;
		ScriptObject.CDOClassIndex = ScriptObjectEntry.CDOClassIndex;
	}
}

FPackageLocalObjectRef FIoStorePackageMap::ResolvePackageLocalRef( const FPackageObjectIndex& PackageObjectIndex )
{
	FPackageLocalObjectRef Result{};

	if ( PackageObjectIndex.IsExport() )
	{
		Result.bIsExportReference = true;
		Result.ExportIndex = PackageObjectIndex.ToExport();
	}
	else if ( PackageObjectIndex.IsImport() )
	{
		Result.bIsImport = true;

		if ( PackageObjectIndex.IsScriptImport() )
		{
			Result.Import.ScriptImportIndex = PackageObjectIndex;
			Result.Import.bIsScriptImport = true;
		}
		else if ( PackageObjectIndex.IsPackageImport() )
		{
			Result.Import.GlobalImportIndex = PackageObjectIndex;
			Result.Import.bIsPackageImport = true;
		}
		else
		{
			check( PackageObjectIndex.IsNull() );
			Result.Import.bIsNullImport = true;
		}
	}
	else
	{
		check( PackageObjectIndex.IsNull() );
		Result.bIsNull = true;
	}
	return Result;
}

TUniquePtr<FIoStoreReader> CreateIoStoreReader(const TCHAR* Path, const FKeyChain& KeyChain)
{
	FIoStoreEnvironment IoEnvironment;
	IoEnvironment.InitializeFileEnvironment(FPaths::ChangeExtension(Path, TEXT("")));
	TUniquePtr<FIoStoreReader> IoStoreReader(new FIoStoreReader());

	TMap<FGuid, FAES::FAESKey> DecryptionKeys;
	for (const auto& KV : KeyChain.EncryptionKeys)
	{
		DecryptionKeys.Add(KV.Key, KV.Value.Key);
	}
	const FIoStatus Status = IoStoreReader->Initialize(IoEnvironment, DecryptionKeys);
	if (Status.IsOk())
	{
		return IoStoreReader;
	}
	else
	{
		UE_LOG(LogIoStoreTools, Warning, TEXT("Failed creating IoStore reader '%s' [%s]"), Path, *Status.ToString())
		return nullptr;
	}
}

FPackageMapExportBundleEntry* FIoStorePackageMap::ReadExportBundleData( const FPackageId& PackageId, const FIoStoreTocChunkInfo& ChunkInfo, const FIoBuffer& ChunkBuffer )
{
	const uint8* PackageSummaryData = ChunkBuffer.Data();
	const FPackageSummary* PackageSummary = reinterpret_cast<const FPackageSummary*>(PackageSummaryData);

	TArray<FNameEntryId> PackageFNames;
	if (PackageSummary->NameMapNamesSize)
	{
		const uint8* NameMapNamesData = PackageSummaryData + PackageSummary->NameMapNamesOffset;
		const uint8* NameMapHashesData = PackageSummaryData + PackageSummary->NameMapHashesOffset;
		LoadNameBatch(
			PackageFNames,
			TArrayView<const uint8>(NameMapNamesData, PackageSummary->NameMapNamesSize),
			TArrayView<const uint8>(NameMapHashesData, PackageSummary->NameMapHashesSize));
	}

	const TArrayView<const uint8> PackageHeaderDataView(PackageSummaryData + sizeof(FPackageSummary), PackageSummary->CookedHeaderSize - sizeof(FPackageSummary));
	FMemoryReaderView PackageHeaderDataReader(PackageHeaderDataView);
	
	// Find package header to resolve imported package IDs
	const FPackageHeaderData& PackageHeader = PackageHeaders.FindChecked( PackageId );
	
	// Construct package data
	FPackageMapExportBundleEntry& PackageData = PackageInfos.AddDefaulted_GetRef();
	PackageData.PackageId = PackageId;
	PackageData.CookedHeaderSize = PackageSummary->CookedHeaderSize;
	PackageData.PackageName = FName::CreateFromDisplayId(PackageFNames[PackageSummary->Name.GetIndex()], PackageSummary->Name.GetNumber());
	PackageData.PackageFlags = PackageSummary->PackageFlags;
	// PackageData.VersioningInfo = VersioningInfo;
	PackageData.PackageChunkId = ChunkInfo.Id;
	
	// Save name map
	PackageData.NameMap.AddZeroed( PackageFNames.Num() );
	for ( int32 i = 0; i < PackageFNames.Num(); i++ )
	{
		PackageData.NameMap[i] = FName::CreateFromDisplayId(PackageFNames[PackageSummary->Name.GetIndex()], NAME_NO_NUMBER_INTERNAL);
	}

	// Resolve import map now
	const FPackageObjectIndex* ImportMap = reinterpret_cast<const FPackageObjectIndex*>(PackageSummaryData + PackageSummary->ImportMapOffset);
	PackageData.ImportMap.SetNum((PackageSummary->ExportMapOffset - PackageSummary->ImportMapOffset) / sizeof(FPackageObjectIndex));
	
	for (int32 ImportIndex = 0; ImportIndex < PackageData.ImportMap.Num(); ++ImportIndex)
	{
		const FPackageObjectIndex& ImportMapEntry = ImportMap[ImportIndex];
		FPackageMapImportEntry& PackageMapImport = PackageData.ImportMap[ImportIndex];
		
		if ( ImportMapEntry.IsScriptImport() )
		{
			PackageMapImport.bIsScriptImport = true;
			PackageMapImport.ScriptImportIndex = ImportMapEntry;
		}
		else if ( ImportMapEntry.IsPackageImport() )
		{
			PackageMapImport.bIsPackageImport = true;
			PackageMapImport.GlobalImportIndex = ImportMapEntry;
		}
		else
		{
			check( ImportMapEntry.IsNull() );
			PackageMapImport.bIsNullImport = true;
		}
	}
	
	const FExportMapEntry* ExportMap = reinterpret_cast<const FExportMapEntry*>(PackageSummaryData + PackageSummary->ExportMapOffset);
	PackageData.ExportMap.SetNum( PackageHeader.ExportCount );
	
	for (int32 ExportIndex = 0; ExportIndex < PackageData.ExportMap.Num(); ++ExportIndex)
	{
		const FExportMapEntry& ExportMapEntry = ExportMap[ ExportIndex ];
		FPackageMapExportEntry& ExportData = PackageData.ExportMap[ ExportIndex ];

		ExportData.ObjectName = FName::CreateFromDisplayId(PackageFNames[ExportMapEntry.ObjectName.GetIndex()], ExportMapEntry.ObjectName.GetNumber());
		ExportData.FilterFlags = ExportMapEntry.FilterFlags;
		ExportData.ObjectFlags = ExportMapEntry.ObjectFlags;

		ExportData.OuterIndex = ResolvePackageLocalRef( ExportMapEntry.OuterIndex );
		ExportData.ClassIndex = ResolvePackageLocalRef( ExportMapEntry.ClassIndex );
		ExportData.SuperIndex = ResolvePackageLocalRef( ExportMapEntry.SuperIndex );
		ExportData.TemplateIndex = ResolvePackageLocalRef( ExportMapEntry.TemplateIndex );
		ExportData.GlobalImportIndex = ResolvePackageLocalRef( ExportMapEntry.GlobalImportIndex );

		ExportData.SerialDataSize = ExportMapEntry.CookedSerialSize;
		ExportData.SerialDataOffset = INDEX_NONE;
	}

	// Read export bundles
	const FExportBundleHeader* ExportBundleHeaders = reinterpret_cast<const FExportBundleHeader*>(PackageSummaryData + PackageSummary->ExportBundlesOffset);
	const FExportBundleEntry* ExportBundleEntries = reinterpret_cast<const FExportBundleEntry*>(ExportBundleHeaders + PackageHeader.ExportBundleCount);
	uint64 CurrentExportOffset = PackageSummary->CookedHeaderSize;
	
	for ( int32 ExportBundleIndex = 0; ExportBundleIndex < PackageHeader.ExportBundleCount; ExportBundleIndex++ )
	{
		TArray<FExportBundleEntry>& ExportBundles = PackageData.ExportBundles.AddDefaulted_GetRef();
		const FExportBundleHeader* ExportBundle = ExportBundleHeaders + ExportBundleIndex;
		
		const FExportBundleEntry* BundleEntry = ExportBundleEntries + ExportBundle->FirstEntryIndex;
		const FExportBundleEntry* BundleEntryEnd = BundleEntry + ExportBundle->EntryCount;
		check(BundleEntry <= BundleEntryEnd);
		
		while (BundleEntry < BundleEntryEnd)
		{
			ExportBundles.Add( *BundleEntry );
			
			if (BundleEntry->CommandType == FExportBundleEntry::ExportCommandType_Serialize)
			{
				FPackageMapExportEntry& Export = PackageData.ExportMap[ BundleEntry->LocalExportIndex ];
				Export.SerialDataOffset = CurrentExportOffset;
				CurrentExportOffset += Export.SerialDataSize;
			}
			BundleEntry++;
		}
	}
	
	TMap<FPackageId, FName> PackageNameMap;
	for (auto& PackageInfo : PackageInfos)
	{
		if (PackageInfo.PackageName != NAME_None)
		{
			PackageNameMap.Add(PackageInfo.PackageId, PackageInfo.PackageName);
		}
		if (PackageInfo.PackageId.IsValid())
		{
			for (int i = 0; i < PackageInfo.ExportMap.Num(); i++)
			{
				auto& Export = PackageInfo.ExportMap[i];
				if (!Export.GlobalImportIndex.bIsNull)
				{
					this->ExportIndices.Add(ExportMap[i].GlobalImportIndex, i);
				}
			}
		}
	}
	
	for (int i = 0; i < PackageNameMap.Num(); i++)
	{
		auto& PackageInfo = PackageInfos[i];
		if (!PackageInfo.PackageId.IsValid()) return nullptr;

		for (int j = 0; j < PackageInfo.ExportMap.Num(); j++)
		{
			auto& Export = PackageInfo.ExportMap[j];

			if (Export.FullName.IsNone())
			{
				TArray<FPackageMapExportEntry*> ExportStack;

				auto* Current = &Export;
				TStringBuilder<2048> FullNameBuilder;
				TCHAR NameBuffer[FName::StringBufferSize];
				for (;;)
				{
					if (!Current->FullName.IsNone())
					{
						Current->FullName.ToString(NameBuffer);
						FullNameBuilder.Append(NameBuffer);
						break;
					}
					ExportStack.Push(Current);
					if (Current->OuterIndex.bIsNull)
					{
						PackageInfo.PackageName.ToString(NameBuffer);
						FullNameBuilder.Append(NameBuffer);
						break;
					}
					Current = &PackageInfo.ExportMap[Current->OuterIndex.ExportIndex];
				}
				while (ExportStack.Num() > 0)
				{
					Current = ExportStack.Pop(false);
					FullNameBuilder.Append(TEXT("/"));
					Current->ObjectName.ToString(NameBuffer);
					FullNameBuilder.Append(NameBuffer);
					Current->FullName = FName(FullNameBuilder);
				}
			}
		}
	}
	
	// Read arcs, they are needed to create a list of preload dependencies for this package
	//const uint64 ExportBundleHeadersSize = sizeof(FExportBundleHeader) * PackageHeader.ExportBundleCount;
	const uint64 ArcsDataOffset = PackageSummary->GraphDataOffset;
	const uint64 ArcsDataSize = PackageSummary->GraphDataSize;

	FMemoryReaderView ArcsAr(MakeArrayView<const uint8>(PackageSummaryData + ArcsDataOffset, ArcsDataSize));

	int32 ImportedPackagesCount;
	ArcsAr << ImportedPackagesCount;

	for ( int32 ImportPackageIndex = 0; ImportPackageIndex < ImportedPackagesCount; ImportPackageIndex++ )
	{
		FPackageMapExternalDependencyArc& ExternalArc = PackageData.ExternalArcs.AddDefaulted_GetRef();
		ArcsAr << ExternalArc.ImportedPackageId;
		ArcsAr << ExternalArc.ExternalArcCount;

		for ( int32 Idx = 0; Idx < ExternalArc.ExternalArcCount; Idx++ )
		{
			FArc NewArc;
			ArcsAr << NewArc;
			ExternalArc.Arcs.Add(NewArc);
		}
	}
	return &PackageData;
}
