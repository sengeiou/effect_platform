/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.TimeUtil;

/**
 * XML文件生成类
 * 
 * @author jiuling.ypf
 * 
 */
public class XMLGenerator {

    private static final Log LOG = LogFactory.getLog(XMLGenerator.class);

    private Document document = null;

    private DOMSource domSource = null;

    private Transformer transFormer = null;

    private StreamResult xmlResult = null;

    private String outputDirEntry = outputDirPrefix + TimeUtil.getTodayDir() + "/";

    private static String outputDirPrefix = "/home/lz/effect_platform/conf/report/";

    private static String outputFilePerfix = "report_";

    private static String outputFileSuffix = ".xml";

    private int id = 0;

    private File file = null;

    private FileOutputStream out = null;

    /**
     * 静态加载
     */
    static {
        // 加载XML配置信息
        outputDirPrefix = ConfigLoader.getXmlOutputDirPrefix();
        LOG.info("config xml output dir prefix = " + outputDirPrefix);
        String outputFileFormat = ConfigLoader.getXmlOutputFileFormat();
        String[] outputFileTokens = outputFileFormat.split("\\*");
        if (outputFileTokens.length == 2) {
            outputFilePerfix = outputFileTokens[0];
            LOG.info("config xml output file perfix = " + outputFilePerfix);
            outputFileSuffix = outputFileTokens[1];
            LOG.info("config xml output file suffix = " + outputFileSuffix);
        }
    }

    /**
     * 构造函数
     * 
     * @param dirEntry
     * @param planId
     */
    public XMLGenerator(String dirEntry, int planId) {
        this.outputDirEntry = dirEntry;
        this.id = planId;
        initDocument();
    }

    /**
     * 构造函数
     * 
     * @param planId
     */
    public XMLGenerator(int planId) {
        this.id = planId;
        initDocument();
    }

    /**
     * 初始化XML Document
     */
    private void initDocument() {
        try {
            DocumentBuilderFactory dFact = DocumentBuilderFactory.newInstance();
            DocumentBuilder build = dFact.newDocumentBuilder();
            document = build.newDocument();
            TransformerFactory tFact = TransformerFactory.newInstance();
            transFormer = tFact.newTransformer();
            transFormer.setOutputProperty(OutputKeys.ENCODING, "UTF-8"); // 设置编码
            transFormer.setOutputProperty(OutputKeys.INDENT, "yes");// 设置缩进
            domSource = new DOMSource(document);
            File dir = new File(outputDirEntry);
            if (!dir.isDirectory()) { // 创建必要的目录
                boolean flag = dir.mkdirs();
                if (!flag) { // 创建失败则切换到当前用户主目录
                    String userHomeDir = System.getProperty("user.home");
                    LOG.warn("fail to create dir: " + outputDirEntry);
                    outputDirEntry = userHomeDir + "/" + TimeUtil.getTodayDir() + "/";
                    LOG.warn("switch to default dir: " + outputDirEntry);
                    dir = new File(outputDirEntry);
                    if (!dir.isDirectory()) {
                        dir.mkdirs();
                    }
                }
            }
            file = new File(outputDirEntry + outputFilePerfix + id + outputFileSuffix);
            if (!file.exists()) { // 生成XML文件
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    LOG.error("fail to create new xml file for plan_id: " + id);
                }
            } else {
                LOG.warn("plan_id: " + id + " xml file has already been generated before, recreate it again");
            }
            try {
                out = new FileOutputStream(file); // 文件输出流
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            xmlResult = new StreamResult(out); // 设置输入源
        } catch (TransformerException ex) {
            LOG.error("TransformerException: error outputting document");
        } catch (ParserConfigurationException ex) {
            LOG.error("ParserConfigurationException: error building document");
        }
    }

    /**
     * 创建不带内容的节点
     * 
     * @param parent
     * @param key
     * @return
     */
    public Element createElement(Element parent, String key) {
        Element element = document.createElement(key);
        if (parent == null) {
            document.appendChild(element);
        } else {
            parent.appendChild(element);
        }
        return element;
    }

    /**
     * 创建不带内容的节点，附加属性信息
     * 
     * @param parent
     * @param key
     * @return
     */
    public Element createElement(Element parent, String key, List<KeyValuePair> attrList) {
        Element element = document.createElement(key);
        for (int i = 0; i < attrList.size(); i++) {
            KeyValuePair pair = (KeyValuePair) (attrList.get(i));
            String name = pair.getKey();
            String value = pair.getValue();
            element.setAttribute(name, value);
        }
        if (parent == null) {
            document.appendChild(element);
        } else {
            parent.appendChild(element);
        }
        return element;
    }

    /**
     * 创建带内容的节点
     * 
     * @param parent
     * @param key
     * @param value
     */
    public Element createElement(Element parent, String key, String value) {
        Element element = document.createElement(key);
        if (parent == null) {
            element.appendChild(document.createTextNode(value));
            document.appendChild(element);
        } else {
            element.appendChild(document.createTextNode(value));
            parent.appendChild(element);
        }
        return element;
    }

    /**
     * 创建带CDATA内容的节点 CDATA区段包含了不会被解析器解析的文本。CDATA区段中的标签不会被视为标记，同时实体也不会被展开。
     * 主要的目的是为了包含诸如 XML片段之类的材料，而无需转义所有的分隔符。 在一个 CDATA 中唯一被识别的分隔符是 "]]>"，它可标示
     * CDATA区段的结束。
     * 
     * @param parent
     * @param key
     * @param value
     */
    public Element createElementCDATAValue(Element parent, String key, String value) {
        Element element = document.createElement(key);
        if (parent == null) {
            element.appendChild(document.createCDATASection(value));
            document.appendChild(element);
        } else {
            element.appendChild(document.createCDATASection(value));
            parent.appendChild(element);
        }
        return element;
    }

    /**
     * 输出生成XML文件 只能被调用一次！因为会关闭out输出流！
     */
    public boolean outputXMLFile() {
        try {
            transFormer.transform(domSource, xmlResult);
            LOG.info("finish to create new xml file, file path=" + file.getAbsolutePath());
            try {
                out.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return true;
        } catch (TransformerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e);
            return false;
        }
    }

    /**
     * 返回XML文件路径
     * 
     * @return
     */
    public String getFilePath() {
        String filePath = file.getAbsolutePath();
        return filePath;
    }

}
