package miage.bigdata.question;
public class StringAndInt implements Comparable<StringAndInt> {

    private String tag;
    private Integer nbOccurTag;

    public StringAndInt(){};
    
    public StringAndInt(String tag, Integer nbOccur) {
    	this.tag=tag;
    	this.nbOccurTag=nbOccur;
	}
    
    @Override
	public int compareTo(StringAndInt o) {
		return Integer.compare(o.nbOccurTag, this.nbOccurTag);
	}

    @Override
    public String toString() {
        return "[tag=" + tag + ", nbOccurTag=" + nbOccurTag + "]";
    }

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public Integer getNbOccurTag() {
		return nbOccurTag;
	}

	public void setNbOccurTag(Integer nbOccurTag) {
		this.nbOccurTag = nbOccurTag;
	}
    
    

}
