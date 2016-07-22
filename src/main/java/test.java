import java.util.Arrays;

/**
 * Created by chzhenzh on 7/19/2016.
 */
public class test {

    public static void main(String[] args) {
        /*
        String str_packet="{abc}f";
        if(str_packet.indexOf("{")>=0 && str_packet.indexOf("}")>=0 && str_packet.indexOf("d")>=0) {
            System.out.println("is yes");
        }else
        {
            System.out.println(str_packet.indexOf("{"));
            System.out.println(str_packet.indexOf("}"));
            System.out.println(str_packet.indexOf("d"));
            System.out.println("is no");
        }*/
        //concat 2 arrays
        int arr1[] = { 0, 1, 2, 3, 4, 5 };
        int arr2[] = { 0, 10, 20, 30, 40, 50 };

        // copies an array from the specified source array
        int arr3[]=new int[arr1.length+arr2.length-1];
        System.arraycopy(arr1, 0, arr3, 0, arr1.length);
        System.arraycopy(arr2, 1, arr3, arr1.length, arr2.length-1);
        System.out.print(Arrays.toString(arr3));
    }
}
