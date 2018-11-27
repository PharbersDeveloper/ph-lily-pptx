package controllers

import com.pharbers.process.common.{phCommand, phLyFactory}
import javax.inject._
import play.api._
import play.api.mvc._

@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

    def index() = Action { implicit request: Request[AnyContent] =>
        phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(null)
        Ok(views.html.index())
    }
}
