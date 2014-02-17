package nljoin;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;



public class NLJoin {

	public static void main(String[] args) {

		
		String joinAttributeName = args[4];
	    int memorySize = Integer.parseInt(args[3]);

		
		File file =  new 	File(args[0]);
	    File file2 = new 	File(args[1]);
	    File file3 = new 	File(args[2]);
	
	    byte[] result;
	    InputStream input 	= null;
	    InputStream input2 	= null;
	    OutputStream output = null;
	    
	    RelationHeader firstHeader  = new RelationHeader();
	    RelationHeader secondHeader = new RelationHeader();
	    
	    
	    try {
			input = new BufferedInputStream(new FileInputStream(file));
			output = new BufferedOutputStream(new FileOutputStream(file3));
			result = new byte[1024];
			
			readFromFile(result, result.length, input);
			readHeader(input, result, firstHeader);
			input2 = new BufferedInputStream(new FileInputStream(file2));
			readFromFile(result, result.length, input2);
			readHeader(input2, result, secondHeader); 
			result = null;

			
			byte[] OutputBuffer = new byte[firstHeader.getPageSize()];
			byte[] InnerBuffer  = new byte[firstHeader.getPageSize()]; 
			byte[] OuterBuffer;
			
			if((memorySize-2) >= firstHeader.getNumberOfPages()){
				OuterBuffer  = new byte[firstHeader.getNumberOfPages()*firstHeader.getPageSize()];
			}
			else{
				OuterBuffer  = new byte[(memorySize-2)*firstHeader.getPageSize()];
			}
			

			int firRelAttributePosition=-1,secRelAttributePosition = -1 ;
			int firBytesBeforeAtt=0,secBytesBeforeAtt = 0;
			
			for(int i=0;i<firstHeader.getNumberOfAttributes();i++){
				if(firstHeader.getAttributeNames(i).equals(joinAttributeName)){
					firRelAttributePosition = i;										// gereksiz
					break;
				}
				else{
					if(firstHeader.getAttributeTypes(i) == 3){
						firBytesBeforeAtt += firstHeader.getAttributeLengths(i);
					}
					else{
						firBytesBeforeAtt += 4;
					}
				}
			}
			for(int i=0;i<secondHeader.getNumberOfAttributes();i++){
				if(secondHeader.getAttributeNames(i).equals(joinAttributeName)){
					secRelAttributePosition = i;
					break;
				}
				else{
					if(secondHeader.getAttributeTypes(i) == 3){
						secBytesBeforeAtt += secondHeader.getAttributeLengths(i);
					}
					else{
						secBytesBeforeAtt += 4;
					}
				}
			}
			
			readFromFile(OuterBuffer, OuterBuffer.length, input);
			
			int my = 0;
			int relCurrentPage = 0;
			int rel2CurrentPage = 0;
			int outerTupleNumber;
			int innerTupleNumber;
			int last = 0;
			int start1,start2;
			int comparision = 0;
			int outputIndex= 0;
			int k;
			boolean equal;
			int curTuple;
			int j;
			int oo;
			
			while( relCurrentPage < firstHeader.getNumberOfPages() ) {
				
				outerTupleNumber = firstHeader.getNumberOfTuples(relCurrentPage);
				
				for(curTuple = 0;curTuple < outerTupleNumber;curTuple++){
					
					start1 = firBytesBeforeAtt + curTuple*firstHeader.getTupleSize() + (relCurrentPage%(memorySize-2))*firstHeader.getPageSize();
					
					rel2CurrentPage = 0;
					
					input2 = null;
					input2 = new BufferedInputStream(new FileInputStream(file2));
					input2.skip(1024);
		
					
					
					while( rel2CurrentPage < secondHeader.getNumberOfPages() ){
						
						// read one page from second relation
						readFromFile(InnerBuffer, InnerBuffer.length, input2);
						
						innerTupleNumber = secondHeader.getNumberOfTuples(rel2CurrentPage);

						for(j=0;j<innerTupleNumber;j++){
							start2= secBytesBeforeAtt+j*secondHeader.getTupleSize();
							
							equal = true;
							k = 0;
							comparision++;
							while(k < secondHeader.getAttributeLengths(secRelAttributePosition)){
							
								if(InnerBuffer[k+start2] == OuterBuffer[k+start1]){
									k++;		
								}
								else{
									equal = false;
									break;
								}
							}
							if(equal == true){
			
								my++;
								
								if(firstHeader.getTupleSize() + secondHeader.getTupleSize() < (firstHeader.getPageSize() - outputIndex)  ){
									for(oo=0;oo<firstHeader.getTupleSize();oo++){
										OutputBuffer[outputIndex] = OuterBuffer[start1-firBytesBeforeAtt+oo];
										outputIndex++;
									}
									for(oo=0;oo<secondHeader.getTupleSize();oo++){
										OutputBuffer[outputIndex] = InnerBuffer[start2-secBytesBeforeAtt+oo];
										outputIndex++;
									}
								}
								else{
									output.write(OutputBuffer, 0, outputIndex);
									outputIndex = 0;
									for(oo=0;oo<firstHeader.getTupleSize();oo++){
										OutputBuffer[outputIndex] = OuterBuffer[start1-firBytesBeforeAtt+oo];
										outputIndex++;
									}
									for(oo=0;oo<secondHeader.getTupleSize();oo++){
										OutputBuffer[outputIndex] = InnerBuffer[start2-secBytesBeforeAtt+oo];
										outputIndex++;
									}
								}
							}
						}
						rel2CurrentPage++;
					}
				}
				relCurrentPage++;
				if(relCurrentPage -last == memorySize-2   ){
					System.out.println("Pages " + (last+1) + "-" + relCurrentPage + " read");
					System.out.println(comparision + " compared " + my + " joined");
					comparision=0;
					my = 0;
					last = relCurrentPage;

					if(relCurrentPage + memorySize-2 > firstHeader.getNumberOfPages()){
						readFromFile(OuterBuffer, input.available(), input);
					}
					else{
						readFromFile(OuterBuffer, OuterBuffer.length, input);
					}
					
				}
			}
			
			output.write(OutputBuffer, 0, outputIndex);
			output.close();
			if(last != firstHeader.getNumberOfPages() ){
				System.out.println("Pages " + (last+1) + "-" + relCurrentPage + " read");
				System.out.println(comparision + " compared " + my + " joined");	
			}

	    	} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		    return;
	
	}

	public static void readFromFile(byte[] result,int byteArraylength,InputStream input){
		int totalBytesRead = 0 , bytesRemaining = 0 , bytesRead = 0;
		while(totalBytesRead < byteArraylength){
	        	bytesRemaining = byteArraylength - totalBytesRead;
	        	try {
					bytesRead = input.read(result, totalBytesRead, bytesRemaining);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
	        	if (bytesRead > 0){
	        		totalBytesRead = totalBytesRead + bytesRead;
	        	}
		}		
	}
	public static int readHeader(InputStream input,byte[] result,RelationHeader header){
		byte[] tmp = new byte[4] ;
		int totalBytesRead = 0;
		
		for(int i=0;i<3;i++){
			getPartOfArray(result, tmp, 4, totalBytesRead);
			totalBytesRead += 4;
			switch (i) {
			case 0:
				header.setPageSize(byteArrayToInt(tmp));
				break;
			case 1:
				header.setNumberOfPages(byteArrayToInt(tmp));
				break;
			case 2:
				header.setNumberOfAttributes(byteArrayToInt(tmp));
				break;
			}
		}
		header.Create();
		tmp = null;
		tmp = new byte[64];
		for(int i=0;i<header.getNumberOfAttributes();i++){
			getPartOfArray(result, tmp, 64,totalBytesRead );
			header.setAttributeNames(new String(tmp),i);
			totalBytesRead += 64;
		}
		tmp = null;
		tmp = new byte[2];
        for(int i=0;i<header.getNumberOfAttributes();i++){
        	getPartOfArray(result, tmp, 2, totalBytesRead);
        	header.setAttributeTypes(byteArrayToShort(tmp), i);
        	totalBytesRead += 2;
        	getPartOfArray(result, tmp, 2, totalBytesRead);
        	header.setAttributeLengths(byteArrayToShort(tmp), i);
        	totalBytesRead += 2;
        }
        tmp = null;
        tmp = new byte[4];
		for(int i=0;i<header.getNumberOfPages();i++){
			getPartOfArray(result, tmp, 4, totalBytesRead);
			header.setNumberOfTuples(byteArrayToInt(tmp),i);
			totalBytesRead += 4;
		}
		return totalBytesRead;
	}
	public static void getPartOfArray(byte[] byteArray, byte[] result,int len,int start){

		for(int i=0,j=start;i<len;i++,j++){
			result[i] = byteArray[j];
		}
	}
	public static int byteArrayToInt(byte[] b) 
	{
		
		    return (b[0] & 0xFF) 		|
		            (b[1] & 0xFF) << 8 	|
		            (b[2] & 0xFF) << 16 |
		            (b[3] & 0xFF) << 24	;
		
	
	}	
	public static short byteArrayToShort(byte[] b){
		return (short) ((b[0] & 0xFF) 	|
	            (b[1] & 0xFF) << 8) ;
	}
}
