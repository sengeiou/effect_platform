package com.ali.lz.effect.tools.config2xml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Java启动外部可执行程序类
 * 
 * @author jiuling.ypf
 * 
 */
public class OuterExeProcess {

    private static final Log LOG = LogFactory.getLog(OuterExeProcess.class);

    /**
     * 启动执行外部可执行程序
     * 
     * @param command
     */
    public static void execute(String command) {
        try {
            Process proc = Runtime.getRuntime().exec(command);
            InputStreamReader ir = new InputStreamReader(proc.getInputStream());
            LineNumberReader lnr = new LineNumberReader(ir);
            String line;
            while ((line = lnr.readLine()) != null) {
                LOG.info(line);
            }
            if (proc.waitFor() == 0)
                LOG.info("Shell Command: " + command + " execute OK!");
            else
                LOG.info("Shell Command: " + command + " execute FAIL!");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        OuterExeProcess.execute("ls -al");
    }

}
