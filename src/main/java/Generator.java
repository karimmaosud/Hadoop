import java.util.*;

/**
 * Created by karim on 21/09/16.
 */
public class Generator implements Runnable{

    private int startId;
    private int numUsers;

    private Product [] products;
    private HDFSWriter hdfsWriter;


    Generator(int startId, int numUsers, Product [] products, HDFSWriter hdfsWriter){
        this.startId = startId;
        this.numUsers = numUsers;
        this.products = products;
        this.hdfsWriter = hdfsWriter;
    }

    public int getNextRandShoppingFrequecy(int minFrequency, int maxFrequency, Random shoppingFrequenceyRand){
        return shoppingFrequenceyRand.nextInt((maxFrequency-minFrequency)+1) + minFrequency;
    }

    public int getRandomSetSize(int std, int mean, Random setSizeRand){
        double size = setSizeRand.nextGaussian()*std + mean;
        return (int)Math.ceil(size);
    }

    public int getRandomQuantity(int minQuantity, int maxQuantity, Random rand){
        return rand.nextInt((maxQuantity-minQuantity)+1) + minQuantity;
    }

    public Set<Product> generatePreferredProducts(Random setSizeRand){

        Random rand = new Random();
        int setSize = getRandomSetSize(10, 20, setSizeRand);
        Set<Product> preferredProducts = new HashSet<Product>();
        while(preferredProducts.size() < setSize){
            int randIndex = rand.nextInt(products.length);
            preferredProducts.add(products[randIndex]);
        }
        return preferredProducts;

    }

    public void copyFromSet(Set<Product> preferredProducts, ArrayList<Product> selectedProducts){
        Iterator<Product> itr = preferredProducts.iterator();
        while(itr.hasNext()){
            selectedProducts.add(itr.next());
        }
    }

    public void sendLinesToWriter(Set<Product> chosenProducts, int userId, int day){

        Random quantityRandom = new Random();
        Iterator<Product> itr = chosenProducts.iterator();
        while(itr.hasNext()){
            String line = userId + "," + day +"\t\t"+itr.next().price + "\t\t"+getRandomQuantity(1, 10, quantityRandom);
            hdfsWriter.addLineToQueue(line);
        }
    }

    public void sendEndOfThreadIndicator(){
        hdfsWriter.addLineToQueue("-1");
    }


    @Override
    public void run() {

        // two shared randoms for each user.
        Random shoppingFrequenceyRand = new Random();
        Random setSizeRand = new Random();
        for(int i= startId; i<startId+numUsers; i++){

            int shoppingFrequency = getNextRandShoppingFrequecy(1, 10, shoppingFrequenceyRand);
            Set<Product> preferredProducts = generatePreferredProducts(setSizeRand);

            int numPerVisit = preferredProducts.size()/10;

            ArrayList<Product> selectedProducts = new ArrayList<Product>();
            copyFromSet(preferredProducts, selectedProducts);


            for(int j =1; j<=120; j+=shoppingFrequency){

                Set<Product> chosenProducts = new HashSet<Product>();
                Random choosingRandom = new Random();
                while (chosenProducts.size() < numPerVisit){
                    Product p = selectedProducts.get(choosingRandom.nextInt(selectedProducts.size()));
                    chosenProducts.add(p);
                }

                sendLinesToWriter(chosenProducts, i, j);
            }
        }
        sendEndOfThreadIndicator();
    }
}
