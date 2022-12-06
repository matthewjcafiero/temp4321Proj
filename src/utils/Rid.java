package utils;

public class Rid {
	int key;
	int pageId;
	int tupleId;

	/**
	 * Consturctor for Rid
	 *
	 * @param key     the value of the key
	 * @param pageId  the page number of this entry
	 * @param tupleId the tuple number of this entry
	 */
	public Rid(int key, int pageId, int tupleId) {
		this.key = key;
		this.pageId = pageId;
		this.tupleId = tupleId;
	}

}
