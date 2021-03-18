package upf.edu.model;

public final class HashTagCount {
	
	final String hashTag;
	final String lang;
	final Long count;

	public String getHashTag() {
		return hashTag;
	}

	public String getLang() {
		return lang;
	}
	
	public Long getCount() {
		return count;
	}
	
	public HashTagCount(String hashTag, String lang, Long count) {
	    this.hashTag = hashTag;
	    this.lang = lang;
	    this.count = count;
	}
}
