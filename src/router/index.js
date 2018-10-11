import Vue from "vue";
import Router from "vue-router";
import store from "@/store";
import UserApi from "@/services/api/UserApi.js";
import Home from "@/components/Home";
import Job from "@/components/Job";
import NotFound from "@/components/NotFound";

Vue.use(Router);

const router = new Router({
  mode: "history",
  routes: [
    {
      component: Home,
      name: "home",
      path: "/"
    },
    {
      component: Job,
      name: "job",
      meta: {
        requiresAuth: true
      },
      path: "/job/:id"
    },
    {
      component: NotFound,
      path: "*"
    }
  ]
});

router.beforeEach((to, from, next) => {
  if (
    to.matched.some(record => record.meta.requiresAuth) &&
    !store.getters.user
  ) {
    UserApi.getMyProfile().then(me => {
      if (me) {
        store.commit("registerMe", me);
        next();
      } else {
        UserApi.getCasLoginURL().then(url => {
          window.location = url;
        });
      }
    });
  } else {
    next();
  }
});

export default router;
