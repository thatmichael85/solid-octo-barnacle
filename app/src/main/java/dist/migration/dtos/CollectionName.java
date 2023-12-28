package dist.migration.dtos;

public enum CollectionName {
    COLLECTION_ONE("COLLECTION_ONE"),
    COLLECTION_TWO("COLLECTION_ONE");

    private final String collectionName;

    CollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }
}
