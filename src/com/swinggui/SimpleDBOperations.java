/**
 * 
 */
package com.swinggui;

import java.util.ArrayList;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;

/**
 * @author Dany
 *
 */
public class SimpleDBOperations {

	/**
	 * @param args
	 */
	static String newDomain="contactsSimpleDB";
	//static AmazonSimpleDB sdb = getDomain(newDomain);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	
	
	public static ArrayList<ContactBean> retrieveItem()
	{
		ArrayList<ContactBean> contactlist=new ArrayList<ContactBean>();
		String selectString = "select * from "+newDomain;
		
		AmazonSimpleDB sdb = getDomain(newDomain);
		SelectRequest select = new SelectRequest(selectString);
		for(Item item : sdb.select(select).getItems())
		{
			System.out.println("Item Name : "+item.getName());
			ContactBean contactbean = new ContactBean();
			for(Attribute attribute : item.getAttributes())
			{
				System.out.println("Name :"+attribute.getName()+" Value : "+attribute.getValue());
				if(attribute.getName().equals("FirstName"))
					contactbean.setFirstName(attribute.getValue());
				else if(attribute.getName().equals("SecondName"))
					contactbean.setLastName(attribute.getValue());
				else if(attribute.getName().equals("MiddleInitial"))
					contactbean.setMiddleInitial(attribute.getValue());
				else if(attribute.getName().equals("AddressLine1"))
					contactbean.setAddressLine1(attribute.getValue());
				else if(attribute.getName().equals("AddressLine2"))
					contactbean.setAddressLine2(attribute.getValue());
				else if(attribute.getName().equals("City"))
					contactbean.setCity(attribute.getValue());
				else if(attribute.getName().equals("State"))
					contactbean.setState(attribute.getValue());
				else if(attribute.getName().equals("Zip"))
					contactbean.setZip(attribute.getValue());
				else if(attribute.getName().equals("PhoneNumber"))
					contactbean.setPhoneNo(attribute.getValue());
					
			}
			contactlist.add(contactbean);
		}
		
		
		
		return contactlist;
	}
	
	public static ContactBean getSelectedBean(ContactBean contact)
	{
		String selectString = "select * from "+newDomain+ " where FirstName="+contact.getFirstName()+" and SecondName="+contact.getLastName()+" and MiddleInitial="+contact.getMiddleInitial();
		
		AmazonSimpleDB sdb = getDomain(newDomain);
				
		SelectRequest select = new SelectRequest(selectString);
		ArrayList<ContactBean> contactlist=new ArrayList<ContactBean>();

		for(Item item : sdb.select(select).getItems())
		{
			
			System.out.println("Item Name : "+item.getName());
			ContactBean contactbean = new ContactBean();
			for(Attribute attribute : item.getAttributes())
			{
			
			System.out.println("Name :"+attribute.getName()+" Value : "+attribute.getValue());
			if(attribute.getName().equals("FirstName"))
				contactbean.setFirstName(attribute.getValue());
			else if(attribute.getName().equals("SecondName"))
				contactbean.setLastName(attribute.getValue());
			else if(attribute.getName().equals("MiddleInitial"))
				contactbean.setMiddleInitial(attribute.getValue());
			else if(attribute.getName().equals("AddressLine1"))
				contactbean.setAddressLine1(attribute.getValue());
			else if(attribute.getName().equals("AddressLine2"))
				contactbean.setAddressLine2(attribute.getValue());
			else if(attribute.getName().equals("City"))
				contactbean.setCity(attribute.getValue());
			else if(attribute.getName().equals("State"))
				contactbean.setState(attribute.getValue());
			else if(attribute.getName().equals("Zip"))
				contactbean.setZip(attribute.getValue());
			else if(attribute.getName().equals("PhoneNumber"))
				contactbean.setPhoneNo(attribute.getValue());
			}
			contactlist.add(contactbean);

		}
		
		return contactlist.get(0);
	}
	
	
	public static void deleteContact(ContactBean contactBean)
	{
		AmazonSimpleDB sdb=getDomain(newDomain);
		String item=contactBean.getFirstName()+contactBean.getLastName()+contactBean.getMiddleInitial();
        sdb.deleteAttributes(new DeleteAttributesRequest(newDomain, item));


	}
	
	
	public static AmazonSimpleDB getDomain(String domain)
	{
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(new ClasspathPropertiesFileCredentialsProvider());
		Region usWest = Region.getRegion(Regions.US_WEST_2);
		sdb.setRegion(usWest);
		
		boolean createDomain=true;
		//List Domains
		for(String domainName : sdb.listDomains().getDomainNames())
		{
			System.out.println("Domain Name : "+domainName);
			if(domainName.equals(domain))
				createDomain=false;
			else
	            sdb.deleteDomain(new DeleteDomainRequest(domainName));

		}
		
		//Create Domain
		if(createDomain)
		sdb.createDomain(new CreateDomainRequest(newDomain));
		
		return sdb;
	}
	
	
	public static void addContactToSimpleDB(ContactBean contact)
	{
		AmazonSimpleDB sdb=getDomain(newDomain);
		sdb.batchPutAttributes(new BatchPutAttributesRequest(newDomain, addItem(contact)));

	}
	
	public static ArrayList<ReplaceableItem> addItem(ContactBean contact)
	{
		ArrayList<ReplaceableItem> contactList=new ArrayList<ReplaceableItem>();
		
		String itemName=contact.getFirstName()+contact.getLastName()+contact.middleInitial;
		contactList.add(new ReplaceableItem(itemName).withAttributes(
				new ReplaceableAttribute("FirstName", contact.getFirstName(), true),
				new ReplaceableAttribute("SecondName", contact.getLastName(), true),
				new ReplaceableAttribute("MiddleInitial", contact.getMiddleInitial(), true),
				new ReplaceableAttribute("AddressLine1", contact.getAddressLine1(), true),
				new ReplaceableAttribute("AddressLine2", contact.getAddressLine2(), true),
				new ReplaceableAttribute("City", contact.getCity(), true),
				new ReplaceableAttribute("State", contact.getState(), true),
				new ReplaceableAttribute("Zip", contact.getZip(), true),
				new ReplaceableAttribute("PhoneNumber", contact.getPhoneNo(), true)));
		
		return contactList;
	}

}
