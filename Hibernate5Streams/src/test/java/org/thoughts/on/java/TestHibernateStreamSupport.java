package org.thoughts.on.java;

import java.util.List;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.thoughts.on.java.model.Book;
import org.thoughts.on.java.model.BookValue;
import java.util.Date;

public class TestHibernateStreamSupport {

	Logger log = Logger.getLogger(this.getClass().getName());

	private EntityManagerFactory emf;

	@Before
	public void init() {
		emf = Persistence.createEntityManagerFactory("my-persistence-unit");
	}

	@After
	public void close() {
		emf.close();
	}

	@Test
	public void testList() {
		log.info("... testList ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		List<Book> books = session.createQuery("SELECT b FROM Book b", Book.class).list();
		
		for(Book b : books) {
			log.info(b.getTitle() + " was published on " + b.getPublishingDate());
		}
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testListToStream() {
		log.info("... testListToStream ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		List<Book> books = session.createQuery("SELECT b FROM Book b", Book.class).list();
		
		books.stream()
			.map(b -> b.getTitle() + " was published on " + b.getPublishingDate())
			.forEach(m -> log.info(m));
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testStreamEntity() {
		log.info("... testStreamEntity ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		Stream<Book> books = session.createQuery("SELECT b FROM Book b", Book.class).stream();
		
		books.map(b -> b.getTitle() + " was published on " + b.getPublishingDate())
			.forEach(m -> log.info(m));
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testStreamColumns() {
		log.info("... testStreamColumns ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		Stream<Object[]> books = session.createNativeQuery("SELECT b.title, b.publishingDate FROM book b").stream();
		
		books.map(b -> new BookValue((String)b[0], (Date)b[1]))
			.map(b -> b.getTitle() + " was published on " + b.getPublishingDate())
			.forEach(m -> log.info(m));
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testStreamPojos() {
		log.info("... testStreamPojos ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		Stream<BookValue> books = session.createQuery("SELECT new org.thoughts.on.java.model.BookValue(b.title, b.publishingDate) FROM Book b", BookValue.class).stream();
		
		books.map(b -> b.getTitle() + " was published on " + b.getPublishingDate())
			.forEach(m -> log.info(m));
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testListPojo() {
		log.info("... testListPojo ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		List<BookValue> books = session.createQuery("SELECT new org.thoughts.on.java.model.BookValue(b.title, b.publishingDate) FROM Book b", BookValue.class).list();
		
		books.stream().map(b -> b.getTitle() + " was published on " + b.getPublishingDate())
			.forEach(b -> log.info(b));
		
		em.getTransaction().commit();
		em.close();
	}
}
