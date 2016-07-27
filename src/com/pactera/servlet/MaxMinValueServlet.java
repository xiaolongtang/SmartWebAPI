package com.pactera.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.pactera.dao.NodeDAO;
import com.pactera.entity.MaxMinNode;
import com.pactera.util.JsonValueProcessorImpl;

import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;


public class MaxMinValueServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    public MaxMinValueServlet() {
        super();
    }


	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String type=request.getParameter("type");
		String value=request.getParameter("value");
		NodeDAO nDAO=new NodeDAO();
		MaxMinNode mmn=nDAO.getMaxMinValue(type, value);
		
		JsonConfig jsonConfig = new JsonConfig();
        jsonConfig.registerJsonValueProcessor(java.util.Date.class, new JsonValueProcessorImpl());
        
        String json = JSONObject.fromObject(mmn, jsonConfig).toString();
        returnWriter(response, json);
	}
	
    private void returnWriter(HttpServletResponse response, String json) throws IOException {
        response.setContentType("application/json;charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.println(json);
    }

}
