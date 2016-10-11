package com.luoxuehuan.sparkproject.test;



import com.luoxuehuan.sparkproject.dao.ITaskDAO;
import com.luoxuehuan.sparkproject.dao.factory.DAOFactory;
import com.luoxuehuan.sparkproject.domain.Task;

public class TaskDAOTest {

	public static void main(String[] args) {
		
		ITaskDAO taskDAO  = DAOFactory.getTaskDAO();
		Task task  = taskDAO.findById(2);
		System.out.println(task.getTaskName());
		
		System.out.println("---");
	}

}
