package com.pactera.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.pactera.dao.NodeDAO;
import com.pactera.entity.DataNode;
import com.pactera.util.JsonValueProcessorImpl;

import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;

/**
 * Servlet implementation class GetValueServlet
 */

public class GetValueServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       

    public GetValueServlet() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String type=request.getParameter("type");
		NodeDAO nDAO=new NodeDAO();
		DataNode dn=nDAO.getLatestValue(type);
		JsonConfig jsonConfig = new JsonConfig();
        jsonConfig.registerJsonValueProcessor(java.util.Date.class, new JsonValueProcessorImpl());
        
        String json = JSONObject.fromObject(dn, jsonConfig).toString();
        returnWriter(response, json);
	}

    private void returnWriter(HttpServletResponse response, String json) throws IOException {
        response.setContentType("application/json;charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.println(json);
    }
}
