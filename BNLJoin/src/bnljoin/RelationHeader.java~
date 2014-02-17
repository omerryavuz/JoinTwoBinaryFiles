package ceng351.phw2;

public class RelationHeader {
		private int 		pageSize,numberOfPages,numberOfAttributes;
		private String[] 	attributeNames;
		private short[] 	attributeTypes;
		private short[] 	attributeLengths;
		private int[]  	numberOfTuples;
		public void Create(){
			this.attributeNames = new String[this.numberOfAttributes];
			this.attributeLengths = new short[this.numberOfAttributes];
			this.attributeTypes = new short[this.numberOfAttributes];
			this.numberOfTuples = new int[this.numberOfPages];
		}
		public int getPageSize() {
			return pageSize;
		}
		public void setPageSize(int pageSize) {
			this.pageSize = pageSize;
		}
		public int getNumberOfPages() {
			return numberOfPages;
		}
		public void setNumberOfPages(int numberOfPages) {
			this.numberOfPages = numberOfPages;
		}
		public int getNumberOfAttributes() {
			return numberOfAttributes;
		}
		public void setNumberOfAttributes(int numberOfAttributes) {
			this.numberOfAttributes = numberOfAttributes;
		}
		
		
		public String[] getAttributeNames() {
			return attributeNames;
		}
		public String getAttributeNames(int i) {
			return attributeNames[i];
		}

		public void setAttributeNames(String attributeName , int position) {
			int i;
			for(i=0;i<attributeName.length();i++){
				if(attributeName.charAt(i) == 0){
					break;
				}
			}
			this.attributeNames[position] = attributeName.substring(0, i);
		}

		public short getAttributeTypes(int i) {
			return attributeTypes[i];
		}

		public void setAttributeTypes(short attributeTypes,int position) {
			this.attributeTypes[position] = attributeTypes;
		}

		public short getAttributeLengths(int i) {
			if(this.attributeTypes[i] == 3){
				return attributeLengths[i];
			}
			return 4;
		}

		public void setAttributeLengths(short attributeLengths,int position) {
			this.attributeLengths[position] = attributeLengths;
		}

		public int getNumberOfTuples(int i) {
			return numberOfTuples[i];
			
		}

		public void setNumberOfTuples(int numberOfTuples,int position) {
			this.numberOfTuples[position] = numberOfTuples;
		}
		public int getTupleSize(){
			int size = 0;
			for(int i=0;i<this.numberOfAttributes;i++){
				if(this.attributeTypes[i] == 3){
					size += this.attributeLengths[i];
				}
				else{
					size += 4;
				}
			}
			return size;
		}
}
