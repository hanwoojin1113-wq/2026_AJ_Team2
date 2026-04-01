package movie.web.login;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@SpringBootTest(properties = "kobis.poster-fetch.enabled=false")
class LoginApplicationTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void contextLoads() {
	}

	@Test
	void loginPageLoads() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(get("/"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/login"));
	}

	@Test
	void signupPageLoads() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(get("/signup"))
				.andExpect(status().isOk())
				.andExpect(view().name("signup-page"));
	}

	@Test
	void registeredUserCanEnterChartsAndMovieDetail() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		mockMvc.perform(get("/charts").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("index"))
				.andExpect(model().attributeExists("movies"))
				.andExpect(model().attributeExists("currentPage"))
				.andExpect(model().attributeExists("totalPages"));

		mockMvc.perform(get("/movies/20129370").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("movie-detail"))
				.andExpect(model().attributeExists("movie"))
				.andExpect(model().attributeExists("genres"))
				.andExpect(model().attributeExists("directors"))
				.andExpect(model().attributeExists("actors"))
				.andExpect(model().attributeExists("companies"))
				.andExpect(model().attributeExists("audits"));
	}

	@Test
	void invalidUserStaysOnLoginPage() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(post("/login")
						.param("id", "unknown")
						.param("pw", "badpw"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/login?error=1"));
	}

	@Test
	void signInCreatesUserInUserTable() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(post("/signup")
						.param("id", "newuser")
						.param("pw", "4321")
						.param("gender", "FEMALE")
						.param("age", "27"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/login"));

		Integer createdCount = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM "USER"
				WHERE login_id = 'newuser'
				  AND login_pw = '4321'
				  AND gender = 'FEMALE'
				  AND age = 27
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(createdCount).isEqualTo(1);
	}

	@Test
	void chartUsesTenMoviesPerPageAcrossEntireMovieList() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		mockMvc.perform(get("/charts?page=2").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("index"))
				.andExpect(model().attributeExists("movies"))
				.andExpect(model().attributeExists("currentPage"))
				.andExpect(model().attributeExists("totalPages"));

		Integer totalMovieCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie", Integer.class);
		Integer pageTwoMovieCount = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM (
					SELECT id
					FROM movie
					ORDER BY ranking ASC
					LIMIT 10 OFFSET 10
				) page_two_movies
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(totalMovieCount).isGreaterThan(10);
		org.assertj.core.api.Assertions.assertThat(pageTwoMovieCount).isEqualTo(10);
	}

}
