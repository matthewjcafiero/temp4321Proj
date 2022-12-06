package utils;

import java.util.Comparator;

/**
 * A comparator for the Rid class
 *
 */
public class RidComparator implements Comparator<Rid> {

	/**
	 * compares two Rid objects based on key, then on pageId, then on tupleId
	 */
	@Override
	public int compare(Rid o1, Rid o2) {
		int res = Integer.compare(o1.key, o2.key);
		if (res != 0)
			return res;
		res = Integer.compare(o1.pageId, o2.pageId);
		if (res != 0)
			return res;
		res = Integer.compare(o1.tupleId, o2.tupleId);
		return res;
	}

}
