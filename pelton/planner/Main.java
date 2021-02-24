import test.DataFlowGraphLibrary;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class Main {
  public static void main(String[] args) {
    DataFlowGraphLibrary.DataFlowGraphGenerator generator = new DataFlowGraphLibrary.DataFlowGraphGenerator();
    generator.AddInputNode("hello-world-table");
  }
}
