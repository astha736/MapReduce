package alljobs;

public class CustomCompare implements Comparable{
   private String str;
   private double tfidf;

   public CustomCompare(String str, double tfidf) {
       setString(str);
       setDouble(tfidf);
   }

   
   public String getString() {
       return this.str;
   }

   private void setString(String str) {
       this.str = str;
   }

   public double getDouble() {
       return this.tfidf;
   }

   private void setDouble(double tfidf) {
       this.tfidf = tfidf;
   }
   
   @Override 
   public int compareTo(Object obj1) {

       if (this.tfidf == ((CustomCompare) obj1).tfidf)
            return 0;
       else if (this.tfidf > ((CustomCompare) obj1).tfidf)
            return -1;
       else
            return 1;
       
    }
}
