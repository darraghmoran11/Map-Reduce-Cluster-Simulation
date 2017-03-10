
public class Test {
    public static void main(String[] args) {
        String text = "Boobs!%&*(";
        for (int i=0; i<text.length(); i++) {
            if (!Character.isLetter(text.charAt(i))) {
                System.out.println(text.charAt(i));
            } else {
                System.out.println("LETTER");
            }
        }
    }
}
