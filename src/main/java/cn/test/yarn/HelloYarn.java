package cn.test.yarn;

public class HelloYarn {
	private String name;
	public HelloYarn(String name) {
		this.name = name;
	}
	
	public void start() {
		println();
	}
	public void println() {
		int i=0;
		while(true){
			i++;
			System.out.println("name:"+name+", "+i);
		}
	}
	
}
