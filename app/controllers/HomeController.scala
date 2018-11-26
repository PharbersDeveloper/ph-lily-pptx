package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import process.common.{phCommand, phLyFactory}

@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

    def index() = Action { implicit request: Request[AnyContent] =>
        phLyFactory.getInstance("process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec
        Ok(views.html.index())
    }
}
